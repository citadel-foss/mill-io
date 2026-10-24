//! The client side of HTTP/1.1, as far as JSON-RPC needs it: a request writer and an
//! incremental response parser.
//!
//! The parser takes bytes in reads of any size and copies body bytes once, so a body
//! arriving a byte at a time costs no more than one arriving whole.

use base64::Engine as _;
use std::fmt;

/// Most bytes in a response's status line and headers, or in its trailers.
pub(crate) const MAX_HEAD_BYTES: usize = 64 * 1024;
/// Longest chunk-size line, extensions included.
const MAX_CHUNK_LINE: usize = 4096;

/// Appends a `POST` of `body` to `path`.
pub(crate) fn write_post(
    out: &mut Vec<u8>,
    path: &str,
    host: &str,
    authorization: Option<&str>,
    body: &[u8],
) {
    out.extend_from_slice(b"POST ");
    out.extend_from_slice(path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\nHost: ");
    out.extend_from_slice(host.as_bytes());
    if let Some(authorization) = authorization {
        out.extend_from_slice(b"\r\nAuthorization: ");
        out.extend_from_slice(authorization.as_bytes());
    }
    out.extend_from_slice(b"\r\nContent-Type: application/json\r\nContent-Length: ");
    out.extend_from_slice(body.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n\r\n");
    out.extend_from_slice(body);
}

/// The `Authorization` value for HTTP Basic auth with `credentials`, which is
/// `user:password`.
pub(crate) fn basic_auth(credentials: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    )
}

/// One complete response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Response {
    pub(crate) status: u16,
    /// The server closes the connection after this response.
    pub(crate) close: bool,
    pub(crate) body: Vec<u8>,
}

/// Why a response could not be parsed. The connection cannot be used after one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HttpError {
    Malformed(String),
    HeadTooLarge,
    BodyTooLarge(usize),
    /// The connection closed inside a response.
    Truncated,
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HttpError::Malformed(what) => write!(f, "malformed HTTP response: {}", what),
            HttpError::HeadTooLarge => {
                write!(f, "HTTP response head exceeds {} bytes", MAX_HEAD_BYTES)
            }
            HttpError::BodyTooLarge(cap) => {
                write!(f, "HTTP response body exceeds {} bytes", cap)
            }
            HttpError::Truncated => write!(f, "connection closed inside an HTTP response"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    StatusLine,
    Headers,
    Body { remaining: usize },
    ChunkSize,
    ChunkData { remaining: usize },
    ChunkEnd,
    Trailers,
    UntilClose,
    Failed,
}

/// Parses responses from a byte stream, carrying partial input across reads.
pub(crate) struct ResponseParser {
    max_body: usize,
    phase: Phase,
    /// The line being read, without its `\n`.
    line: Vec<u8>,
    /// Head or trailer bytes read so far.
    head_bytes: usize,
    status: u16,
    http_1_0: bool,
    content_length: Option<usize>,
    chunked: bool,
    other_coding: bool,
    close: bool,
    keep_alive: bool,
    body: Vec<u8>,
}

impl ResponseParser {
    /// A parser that refuses bodies over `max_body` bytes.
    pub(crate) fn new(max_body: usize) -> Self {
        Self {
            max_body,
            phase: Phase::StatusLine,
            line: Vec::new(),
            head_bytes: 0,
            status: 0,
            http_1_0: false,
            content_length: None,
            chunked: false,
            other_coding: false,
            close: false,
            keep_alive: false,
            body: Vec::new(),
        }
    }

    /// True between responses, with no bytes of the next one read.
    pub(crate) fn is_idle(&self) -> bool {
        self.phase == Phase::StatusLine && self.line.is_empty()
    }

    /// Feeds `data`, pushing each response it completes onto `out`. After an error the
    /// parser fails every call.
    pub(crate) fn push(&mut self, data: &[u8], out: &mut Vec<Response>) -> Result<(), HttpError> {
        let result = self.consume(data, out);
        if result.is_err() {
            self.phase = Phase::Failed;
        }
        result
    }

    /// The peer closed the connection: completes a body that runs until close. `None`
    /// when no response was under way.
    pub(crate) fn finish(&mut self) -> Result<Option<Response>, HttpError> {
        match self.phase {
            Phase::UntilClose => Ok(Some(self.complete())),
            _ if self.is_idle() => Ok(None),
            _ => {
                self.phase = Phase::Failed;
                Err(HttpError::Truncated)
            }
        }
    }

    fn consume(&mut self, mut data: &[u8], out: &mut Vec<Response>) -> Result<(), HttpError> {
        while !data.is_empty() {
            match self.phase {
                Phase::Failed => {
                    return Err(HttpError::Malformed("parser already failed".into()));
                }
                Phase::Body { remaining } => {
                    let n = remaining.min(data.len());
                    self.body.extend_from_slice(&data[..n]);
                    data = &data[n..];
                    if n == remaining {
                        out.push(self.complete());
                    } else {
                        self.phase = Phase::Body {
                            remaining: remaining - n,
                        };
                    }
                }
                Phase::ChunkData { remaining } => {
                    let n = remaining.min(data.len());
                    self.body.extend_from_slice(&data[..n]);
                    data = &data[n..];
                    self.phase = if n == remaining {
                        Phase::ChunkEnd
                    } else {
                        Phase::ChunkData {
                            remaining: remaining - n,
                        }
                    };
                }
                Phase::UntilClose => {
                    if self.body.len() + data.len() > self.max_body {
                        return Err(HttpError::BodyTooLarge(self.max_body));
                    }
                    self.body.extend_from_slice(data);
                    data = &[];
                }
                Phase::StatusLine | Phase::Headers | Phase::Trailers => {
                    let limit = MAX_HEAD_BYTES - self.head_bytes.min(MAX_HEAD_BYTES);
                    let Some(taken) = self.take_line(&mut data, limit, HttpError::HeadTooLarge)?
                    else {
                        return Ok(());
                    };
                    self.head_bytes += taken;
                    let raw = std::mem::take(&mut self.line);
                    let line = strip_cr(&raw);
                    match self.phase {
                        Phase::StatusLine => self.status_line(line)?,
                        Phase::Headers if line.is_empty() => self.end_of_head(out)?,
                        Phase::Headers => self.header(line)?,
                        _ if line.is_empty() => out.push(self.complete()),
                        _ => {}
                    }
                    self.line = raw;
                    self.line.clear();
                }
                Phase::ChunkSize | Phase::ChunkEnd => {
                    let too_long = HttpError::Malformed("chunk-size line too long".into());
                    let Some(_) = self.take_line(&mut data, MAX_CHUNK_LINE, too_long)? else {
                        return Ok(());
                    };
                    let raw = std::mem::take(&mut self.line);
                    if self.phase == Phase::ChunkEnd {
                        if !strip_cr(&raw).is_empty() {
                            return Err(HttpError::Malformed(
                                "chunk data overruns its size".into(),
                            ));
                        }
                        self.phase = Phase::ChunkSize;
                    } else {
                        self.chunk_size(strip_cr(&raw))?;
                    }
                    self.line = raw;
                    self.line.clear();
                }
            }
        }
        Ok(())
    }

    /// Moves bytes up to and including the next `\n` from `data` into `self.line`. Returns
    /// how many bytes the line took once it is complete, `None` while it is not.
    fn take_line(
        &mut self,
        data: &mut &[u8],
        limit: usize,
        too_long: HttpError,
    ) -> Result<Option<usize>, HttpError> {
        let (part, complete) = match data.iter().position(|&b| b == b'\n') {
            Some(end) => (&data[..end], true),
            None => (*data, false),
        };
        if self.line.len() + part.len() + usize::from(complete) > limit {
            return Err(too_long);
        }
        self.line.extend_from_slice(part);
        *data = &data[part.len() + usize::from(complete)..];
        Ok(complete.then_some(self.line.len() + 1))
    }

    fn status_line(&mut self, line: &[u8]) -> Result<(), HttpError> {
        let malformed = || HttpError::Malformed(format!("status line {:?}", lossy(line)));
        let rest = line.strip_prefix(b"HTTP/1.").ok_or_else(malformed)?;
        let (minor, rest) = rest.split_first().ok_or_else(malformed)?;
        let code = match rest {
            [b' ', a, b, c, ..] if rest.len() == 4 || rest[4] == b' ' => [*a, *b, *c],
            _ => return Err(malformed()),
        };
        if !minor.is_ascii_digit() || !code.iter().all(u8::is_ascii_digit) {
            return Err(malformed());
        }
        self.status = code
            .iter()
            .fold(0u16, |status, digit| status * 10 + u16::from(digit - b'0'));
        self.http_1_0 = *minor == b'0';
        self.phase = Phase::Headers;
        Ok(())
    }

    fn header(&mut self, line: &[u8]) -> Result<(), HttpError> {
        let malformed = || HttpError::Malformed(format!("header line {:?}", lossy(line)));
        let colon = line.iter().position(|&b| b == b':').ok_or_else(malformed)?;
        let name = &line[..colon];
        if name.is_empty() || name.iter().any(|b| b.is_ascii_whitespace()) {
            return Err(malformed());
        }
        let value = trim(&line[colon + 1..]);
        if name.eq_ignore_ascii_case(b"content-length") {
            for part in value.split(|&b| b == b',') {
                let length = parse_length(trim(part)).ok_or_else(malformed)?;
                if self.content_length.is_some_and(|known| known != length) {
                    return Err(HttpError::Malformed("conflicting Content-Length".into()));
                }
                self.content_length = Some(length);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            for coding in value.split(|&b| b == b',').map(trim) {
                let is_chunked = coding.eq_ignore_ascii_case(b"chunked");
                self.other_coding |= !is_chunked && !coding.is_empty();
                self.chunked |= is_chunked;
            }
        } else if name.eq_ignore_ascii_case(b"connection") {
            for option in value.split(|&b| b == b',').map(trim) {
                self.close |= option.eq_ignore_ascii_case(b"close");
                self.keep_alive |= option.eq_ignore_ascii_case(b"keep-alive");
            }
        }
        Ok(())
    }

    fn end_of_head(&mut self, out: &mut Vec<Response>) -> Result<(), HttpError> {
        if self.http_1_0 && !self.keep_alive {
            self.close = true;
        }
        if (100..200).contains(&self.status) {
            self.reset_head();
            return Ok(());
        }
        if self.status == 204 || self.status == 304 {
            out.push(self.complete());
            return Ok(());
        }
        if self.other_coding {
            // A body framed by a coding we cannot decode can only end at close.
            self.close = true;
            self.phase = Phase::UntilClose;
        } else if self.chunked {
            self.head_bytes = 0;
            self.phase = Phase::ChunkSize;
        } else if let Some(length) = self.content_length {
            if length > self.max_body {
                return Err(HttpError::BodyTooLarge(self.max_body));
            }
            self.body.reserve_exact(length);
            if length == 0 {
                out.push(self.complete());
            } else {
                self.phase = Phase::Body { remaining: length };
            }
        } else {
            self.close = true;
            self.phase = Phase::UntilClose;
        }
        Ok(())
    }

    fn chunk_size(&mut self, line: &[u8]) -> Result<(), HttpError> {
        let digits = trim(line.split(|&b| b == b';').next().unwrap_or_default());
        let size = parse_hex(digits)
            .ok_or_else(|| HttpError::Malformed(format!("chunk size {:?}", lossy(line))))?;
        if size == 0 {
            self.phase = Phase::Trailers;
            return Ok(());
        }
        if size > self.max_body - self.body.len().min(self.max_body) {
            return Err(HttpError::BodyTooLarge(self.max_body));
        }
        self.phase = Phase::ChunkData { remaining: size };
        Ok(())
    }

    fn complete(&mut self) -> Response {
        let response = Response {
            status: self.status,
            close: self.close,
            body: std::mem::take(&mut self.body),
        };
        self.reset_head();
        response
    }

    fn reset_head(&mut self) {
        self.phase = Phase::StatusLine;
        self.head_bytes = 0;
        self.status = 0;
        self.http_1_0 = false;
        self.content_length = None;
        self.chunked = false;
        self.other_coding = false;
        self.close = false;
        self.keep_alive = false;
    }
}

fn strip_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn trim(mut s: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = s {
        s = rest;
    }
    while let [rest @ .., b' ' | b'\t'] = s {
        s = rest;
    }
    s
}

fn parse_length(digits: &[u8]) -> Option<usize> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits).ok()?.parse().ok()
}

fn parse_hex(digits: &[u8]) -> Option<usize> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    usize::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(80)]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: usize = 1 << 20;

    fn parse_all(reads: &[&[u8]], cap: usize) -> Result<Vec<Response>, HttpError> {
        let mut parser = ResponseParser::new(cap);
        let mut out = Vec::new();
        for read in reads {
            parser.push(read, &mut out)?;
        }
        Ok(out)
    }

    fn one(wire: &[u8]) -> Response {
        let responses = parse_all(&[wire], CAP).unwrap();
        assert_eq!(responses.len(), 1, "{:?}", responses);
        responses.into_iter().next().unwrap()
    }

    fn error(wire: &[u8]) -> HttpError {
        parse_all(&[wire], CAP).unwrap_err()
    }

    fn response(status: u16, close: bool, body: &[u8]) -> Response {
        Response {
            status,
            close,
            body: body.to_vec(),
        }
    }

    #[test]
    fn writes_a_post_with_auth_and_length() {
        let mut out = b"x".to_vec();
        write_post(
            &mut out,
            "/wallet/w",
            "127.0.0.1:8332",
            Some("Basic abc"),
            b"{}",
        );
        assert_eq!(
            out,
            b"xPOST /wallet/w HTTP/1.1\r\nHost: 127.0.0.1:8332\r\nAuthorization: Basic abc\r\n\
              Content-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
        );
        out.clear();
        write_post(&mut out, "/", "h", None, b"");
        assert_eq!(
            out,
            b"POST / HTTP/1.1\r\nHost: h\r\nContent-Type: application/json\r\n\
              Content-Length: 0\r\n\r\n"
        );
    }

    #[test]
    fn basic_auth_is_base64_of_the_credentials() {
        assert_eq!(basic_auth("user:pass"), "Basic dXNlcjpwYXNz");
        assert_eq!(basic_auth(""), "Basic ");
    }

    #[test]
    fn content_length_body() {
        let r = one(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello");
        assert_eq!(r, response(200, false, b"hello"));
    }

    #[test]
    fn zero_length_and_bodiless_statuses_complete_at_the_head() {
        assert_eq!(
            one(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"),
            response(401, false, b"")
        );
        assert_eq!(
            one(b"HTTP/1.1 204 No Content\r\n\r\n"),
            response(204, false, b"")
        );
        assert_eq!(
            one(b"HTTP/1.1 304 Not Modified\r\n\r\n"),
            response(304, false, b"")
        );
    }

    #[test]
    fn chunked_body_with_extensions_and_trailers() {
        let wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                     5;name=value\r\nhello\r\n1 ; x\r\n \r\nA\r\n0123456789\r\n\
                     0\r\nX-Trailer: yes\r\nOther: 1\r\n\r\n";
        assert_eq!(one(wire), response(200, false, b"hello 0123456789"));
    }

    #[test]
    fn chunk_sizes_are_hex_in_either_case() {
        let wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                     b\r\n0123456789a\r\nB\r\n0123456789a\r\n0\r\n\r\n";
        assert_eq!(one(wire).body, b"0123456789a0123456789a");
    }

    #[test]
    fn bare_newlines_are_accepted() {
        let wire = b"HTTP/1.1 200 OK\nTransfer-Encoding: chunked\n\n2\nok\n0\n\n";
        assert_eq!(one(wire), response(200, false, b"ok"));
    }

    #[test]
    fn transfer_encoding_wins_over_content_length() {
        let wire = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: Chunked\r\n\r\n\
                     2\r\nok\r\n0\r\n\r\n";
        assert_eq!(one(wire).body, b"ok");
    }

    #[test]
    fn header_names_are_case_insensitive_and_values_trimmed() {
        let r = one(b"HTTP/1.1 200 OK\r\ncontent-LENGTH: \t 2 \r\nCONNECTION:  Close \r\n\r\nok");
        assert_eq!(r, response(200, true, b"ok"));
    }

    #[test]
    fn connection_close_and_http_1_0_defaults() {
        let close =
            one(b"HTTP/1.1 200 OK\r\nConnection: keep-alive, close\r\nContent-Length: 0\r\n\r\n");
        assert!(close.close);
        let old = one(b"HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n");
        assert!(old.close);
        let kept = one(b"HTTP/1.0 200 OK\r\nConnection: Keep-Alive\r\nContent-Length: 0\r\n\r\n");
        assert!(!kept.close);
        let upgrade = one(b"HTTP/1.1 200 OK\r\nConnection: upgrade\r\nContent-Length: 0\r\n\r\n");
        assert!(!upgrade.close);
    }

    #[test]
    fn repeated_equal_content_lengths_are_fine_and_conflicting_ones_fail() {
        let r = one(b"HTTP/1.1 200 OK\r\nContent-Length: 2, 2\r\nContent-Length: 2\r\n\r\nok");
        assert_eq!(r.body, b"ok");
        assert!(matches!(
            error(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\n"),
            HttpError::Malformed(_)
        ));
    }

    #[test]
    fn body_without_framing_runs_until_close() {
        let mut parser = ResponseParser::new(CAP);
        let mut out = Vec::new();
        parser
            .push(b"HTTP/1.1 500 Oops\r\n\r\npart one, ", &mut out)
            .unwrap();
        parser.push(b"part two", &mut out).unwrap();
        assert!(out.is_empty());
        assert_eq!(
            parser.finish().unwrap(),
            Some(response(500, true, b"part one, part two"))
        );
        assert!(parser.is_idle());
        assert_eq!(parser.finish().unwrap(), None);
    }

    #[test]
    fn unknown_transfer_coding_runs_until_close() {
        let mut parser = ResponseParser::new(CAP);
        let mut out = Vec::new();
        parser
            .push(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: 1\r\n\r\nabc",
                &mut out,
            )
            .unwrap();
        assert!(out.is_empty());
        assert_eq!(parser.finish().unwrap(), Some(response(200, true, b"abc")));
    }

    #[test]
    fn informational_responses_are_skipped() {
        let wire = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        assert_eq!(one(wire), response(200, false, b"ok"));
    }

    #[test]
    fn back_to_back_responses_in_one_read() {
        let wire = b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\naHTTP/1.1 503 Busy\r\n\
                     Transfer-Encoding: chunked\r\n\r\n1\r\nb\r\n0\r\n\r\nHTTP/1.1 200 OK\r\n";
        let mut parser = ResponseParser::new(CAP);
        let mut out = Vec::new();
        parser.push(wire, &mut out).unwrap();
        assert_eq!(
            out,
            [response(200, false, b"a"), response(503, false, b"b")]
        );
        assert!(!parser.is_idle());
        assert_eq!(parser.finish(), Err(HttpError::Truncated));
    }

    #[test]
    fn head_and_chunk_lines_split_anywhere() {
        let wire: &[u8] = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX: y\r\n\r\n\
                            3;e=1\r\nabc\r\n2\r\nde\r\n0\r\nT: 1\r\n\r\n";
        for split in 0..wire.len() {
            let (a, b) = wire.split_at(split);
            let out = parse_all(&[a, b], CAP).unwrap();
            assert_eq!(out, [response(200, false, b"abcde")], "split at {}", split);
        }
        let bytes: Vec<&[u8]> = wire.chunks(1).collect();
        assert_eq!(
            parse_all(&bytes, CAP).unwrap(),
            [response(200, false, b"abcde")]
        );
    }

    #[test]
    fn idle_only_between_responses() {
        let mut parser = ResponseParser::new(CAP);
        let mut out = Vec::new();
        assert!(parser.is_idle());
        parser.push(b"H", &mut out).unwrap();
        assert!(!parser.is_idle());
        parser
            .push(b"TTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n", &mut out)
            .unwrap();
        assert!(!parser.is_idle());
        parser.push(b"x", &mut out).unwrap();
        assert!(parser.is_idle());
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn truncated_responses_fail_at_close() {
        for wire in [
            &b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhel"[..],
            b"HTTP/1.1 200 OK\r\nContent-Le",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhel",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n",
        ] {
            let mut parser = ResponseParser::new(CAP);
            parser.push(wire, &mut Vec::new()).unwrap();
            assert_eq!(parser.finish(), Err(HttpError::Truncated), "{:?}", wire);
            assert!(parser.push(b"x", &mut Vec::new()).is_err());
        }
    }

    #[test]
    fn content_length_over_the_cap_fails_before_the_body() {
        let wire = b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n";
        assert_eq!(parse_all(&[wire], 10), Err(HttpError::BodyTooLarge(10)));
        let exact = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789";
        assert_eq!(parse_all(&[exact], 10).unwrap()[0].body, b"0123456789");
    }

    #[test]
    fn chunked_bodies_over_the_cap_fail() {
        let wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\n012345\r\n5\r\n";
        assert_eq!(parse_all(&[wire], 10), Err(HttpError::BodyTooLarge(10)));
        let exact = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\
                      6\r\n012345\r\n4\r\n6789\r\n0\r\n\r\n";
        assert_eq!(parse_all(&[exact], 10).unwrap()[0].body, b"0123456789");
        let huge = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffff\r\n";
        assert_eq!(parse_all(&[huge], 10), Err(HttpError::BodyTooLarge(10)));
    }

    #[test]
    fn until_close_bodies_over_the_cap_fail() {
        let mut parser = ResponseParser::new(4);
        let mut out = Vec::new();
        parser
            .push(b"HTTP/1.1 200 OK\r\n\r\n1234", &mut out)
            .unwrap();
        assert_eq!(parser.push(b"5", &mut out), Err(HttpError::BodyTooLarge(4)));
    }

    #[test]
    fn oversized_heads_fail() {
        let mut wire = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
        wire.resize(MAX_HEAD_BYTES + 1, b'a');
        assert_eq!(error(&wire), HttpError::HeadTooLarge);
        let mut ok = b"HTTP/1.1 200 OK\r\nX: ".to_vec();
        ok.resize(MAX_HEAD_BYTES - 25, b'a');
        ok.extend_from_slice(b"\r\nContent-Length: 0\r\n\r\n");
        assert!(ok.len() <= MAX_HEAD_BYTES);
        assert_eq!(one(&ok).status, 200);
        let mut long_status = b"HTTP/1.1 200 ".to_vec();
        long_status.resize(MAX_HEAD_BYTES + 1, b'a');
        assert_eq!(error(&long_status), HttpError::HeadTooLarge);
    }

    #[test]
    fn oversized_trailers_fail() {
        let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nT: ".to_vec();
        wire.resize(wire.len() + MAX_HEAD_BYTES, b'a');
        assert_eq!(error(&wire), HttpError::HeadTooLarge);
    }

    #[test]
    fn oversized_chunk_lines_fail() {
        let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;".to_vec();
        wire.resize(wire.len() + MAX_CHUNK_LINE, b'x');
        assert!(matches!(error(&wire), HttpError::Malformed(_)));
        let mut fits = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;".to_vec();
        fits.resize(fits.len() + MAX_CHUNK_LINE - 4, b'x');
        fits.extend_from_slice(b"\r\nz\r\n0\r\n\r\n");
        assert_eq!(one(&fits).body, b"z");
    }

    #[test]
    fn malformed_status_lines_fail() {
        for line in [
            &b"HTTP/2 200 OK\r\n"[..],
            b"HTTP/1.1 20 OK\r\n",
            b"HTTP/1.1 2000 OK\r\n",
            b"HTTP/1.1 2x0 OK\r\n",
            b"HTTP/1.x 200 OK\r\n",
            b"HTTP/1.1200 OK\r\n",
            b"HTTP/1.\r\n",
            b"ICY 200 OK\r\n",
            b"\r\n",
        ] {
            assert!(matches!(error(line), HttpError::Malformed(_)), "{:?}", line);
        }
        assert_eq!(
            one(b"HTTP/1.1 200\r\nContent-Length: 0\r\n\r\n").status,
            200
        );
        assert_eq!(
            one(b"HTTP/1.1 599 Custom Reason Here\r\nContent-Length: 0\r\n\r\n").status,
            599
        );
    }

    #[test]
    fn malformed_headers_fail() {
        for head in [
            &b"HTTP/1.1 200 OK\r\nno colon\r\n\r\n"[..],
            b"HTTP/1.1 200 OK\r\n: empty name\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nBad Name: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n folded: x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 1x\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: \r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999999\r\n\r\n",
        ] {
            assert!(matches!(error(head), HttpError::Malformed(_)), "{:?}", head);
        }
    }

    #[test]
    fn malformed_chunks_fail() {
        for body in [
            &b"zz\r\n"[..],
            b"\r\n",
            b";ext\r\n",
            b"-1\r\n",
            b"2\r\nabc\r\n",
            b"10000000000000000\r\n",
        ] {
            let mut wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
            wire.extend_from_slice(body);
            assert!(
                matches!(error(&wire), HttpError::Malformed(_)),
                "{:?}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn errors_poison_the_parser() {
        let mut parser = ResponseParser::new(CAP);
        let mut out = Vec::new();
        assert!(parser.push(b"garbage\r\n", &mut out).is_err());
        assert!(parser
            .push(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", &mut out)
            .is_err());
        assert_eq!(parser.finish(), Err(HttpError::Truncated));
        assert!(out.is_empty());
    }

    #[test]
    fn helpers() {
        assert_eq!(trim(b" \t a b \t"), b"a b");
        assert_eq!(trim(b"   "), b"");
        assert_eq!(parse_length(b"0"), Some(0));
        assert_eq!(parse_length(b"+1"), None);
        assert_eq!(parse_hex(b"fF"), Some(255));
        assert_eq!(parse_hex(b"0x1"), None);
        assert_eq!(parse_hex(b""), None);
        assert_eq!(strip_cr(b"a\r"), b"a");
        assert_eq!(strip_cr(b"a\r\r"), b"a\r");
    }

    #[test]
    fn errors_display() {
        assert_eq!(
            HttpError::BodyTooLarge(7).to_string(),
            "HTTP response body exceeds 7 bytes"
        );
        assert_eq!(
            HttpError::HeadTooLarge.to_string(),
            "HTTP response head exceeds 65536 bytes"
        );
        assert_eq!(
            HttpError::Truncated.to_string(),
            "connection closed inside an HTTP response"
        );
        assert_eq!(
            HttpError::Malformed("x".into()).to_string(),
            "malformed HTTP response: x"
        );
    }
}
