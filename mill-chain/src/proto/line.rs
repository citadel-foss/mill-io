//! Line-delimited JSON-RPC: one JSON object per `\n`-terminated line, as Electrum servers
//! speak it.

use crate::engine::{Codec, Reply};
use crate::ChainError;
use serde_json::Value;

/// Longest line kept; longer ones are dropped whole, so a peer cannot grow the buffer
/// without bound.
pub(crate) const DEFAULT_MAX_LINE: usize = 64 * 1024 * 1024;

/// Splits a byte stream into lines, carrying partial lines across calls.
pub(crate) struct LineFramer {
    buf: Vec<u8>,
    max_line: usize,
    /// Inside a line that outgrew `max_line`; bytes are dropped up to its `\n`.
    discarding: bool,
}

impl LineFramer {
    pub(crate) fn new(max_line: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_line,
            discarding: false,
        }
    }

    /// Feeds `data`, calling `emit` with each complete non-empty line, without its `\n`
    /// or a trailing `\r`.
    pub(crate) fn push(&mut self, mut data: &[u8], mut emit: impl FnMut(&[u8])) {
        while let Some(end) = data.iter().position(|&b| b == b'\n') {
            let head = &data[..end];
            if self.discarding {
                self.discarding = false;
            } else if self.buf.is_empty() {
                self.emit_line(head, &mut emit);
            } else {
                let mut line = std::mem::take(&mut self.buf);
                line.extend_from_slice(head);
                self.emit_line(&line, &mut emit);
                line.clear();
                self.buf = line;
            }
            data = &data[end + 1..];
        }
        if self.discarding || data.is_empty() {
            return;
        }
        self.buf.extend_from_slice(data);
        if self.buf.len() > self.max_line {
            log::warn!("dropping a line longer than {} bytes", self.max_line);
            self.buf.clear();
            self.discarding = true;
        }
    }

    pub(crate) fn reset(&mut self) {
        self.buf.clear();
        self.discarding = false;
    }

    fn emit_line(&self, line: &[u8], emit: &mut impl FnMut(&[u8])) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.len() > self.max_line {
            log::warn!("dropping a line longer than {} bytes", self.max_line);
        } else if !line.is_empty() {
            emit(line);
        }
    }
}

/// JSON-RPC 2.0 requests and replies, one per line.
pub(crate) struct LineJsonRpc {
    framer: LineFramer,
}

impl LineJsonRpc {
    pub(crate) fn new() -> Self {
        Self {
            framer: LineFramer::new(DEFAULT_MAX_LINE),
        }
    }
}

impl Codec for LineJsonRpc {
    fn encode(
        &mut self,
        id: u64,
        method: &str,
        params: &Value,
        out: &mut Vec<u8>,
    ) -> Result<(), ChainError> {
        out.extend_from_slice(br#"{"jsonrpc":"2.0","id":"#);
        out.extend_from_slice(id.to_string().as_bytes());
        out.extend_from_slice(br#","method":"#);
        serde_json::to_writer(&mut *out, method)
            .and_then(|()| {
                out.extend_from_slice(br#","params":"#);
                serde_json::to_writer(&mut *out, params)
            })
            .map_err(|e| ChainError::Protocol(format!("cannot encode request: {}", e)))?;
        out.extend_from_slice(b"}\n");
        Ok(())
    }

    fn decode(&mut self, data: &[u8], replies: &mut Vec<Reply>) {
        self.framer.push(data, |line| {
            if let Some(reply) = parse_reply(line) {
                replies.push(reply);
            }
        });
    }

    fn reset(&mut self) {
        self.framer.reset();
    }
}

/// Parses one reply line. `None` if the line cannot be tied to a request: it is not JSON,
/// or has no unsigned integer `id`.
pub(crate) fn parse_reply(line: &[u8]) -> Option<Reply> {
    let value: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(e) => {
            log::warn!("dropping a line that is not JSON: {}", e);
            return None;
        }
    };
    let Value::Object(mut reply) = value else {
        log::debug!("dropping a line that is not a JSON object");
        return None;
    };
    let Some(id) = reply.get("id").and_then(Value::as_u64) else {
        log::debug!("dropping a reply without a request id");
        return None;
    };
    let result = match reply.remove("error") {
        None => reply
            .remove("result")
            .ok_or_else(|| ChainError::Protocol("reply has neither result nor error".into())),
        Some(Value::Null) => Ok(reply.remove("result").unwrap_or(Value::Null)),
        Some(Value::Object(error)) => Err(server_error(&error)),
        Some(other) => Err(ChainError::Protocol(format!(
            "error is not an object: {}",
            other
        ))),
    };
    Some(Reply { id, result })
}

fn server_error(error: &serde_json::Map<String, Value>) -> ChainError {
    let Some(code) = error.get("code").and_then(Value::as_i64) else {
        return ChainError::Protocol("error object has no integer code".into());
    };
    let message = match error.get("message") {
        Some(Value::String(message)) => message.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    };
    ChainError::Server { code, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lines(framer: &mut LineFramer, data: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        framer.push(data, |line| {
            out.push(String::from_utf8(line.to_vec()).unwrap())
        });
        out
    }

    #[test]
    fn framer_joins_partial_lines_and_skips_empty_ones() {
        let mut framer = LineFramer::new(100);
        assert!(lines(&mut framer, b"ab").is_empty());
        assert_eq!(lines(&mut framer, b"c\n\r\n\nde\r\nf"), ["abc", "de"]);
        assert_eq!(lines(&mut framer, b"\n"), ["f"]);
        framer.push(b"partial", |_| panic!("no line yet"));
        framer.reset();
        assert_eq!(lines(&mut framer, b"x\n"), ["x"]);
    }

    #[test]
    fn framer_drops_over_long_lines_and_recovers() {
        let mut framer = LineFramer::new(4);
        assert_eq!(lines(&mut framer, b"12345\nok\n"), ["ok"]);
        assert!(lines(&mut framer, b"123").is_empty());
        assert!(lines(&mut framer, b"45").is_empty());
        assert!(lines(&mut framer, b"6789").is_empty());
        assert_eq!(lines(&mut framer, b"0\n1234\n"), ["1234"]);
        assert!(lines(&mut framer, b"12").is_empty());
        assert_eq!(lines(&mut framer, b"345\nab\n"), ["ab"]);
    }

    #[test]
    fn encodes_one_request_per_line() {
        let mut codec = LineJsonRpc::new();
        let mut out = Vec::new();
        codec
            .encode(7, "get \"tip\"", &json!([1, "a"]), &mut out)
            .unwrap();
        assert_eq!(out.last(), Some(&b'\n'));
        assert_eq!(out.iter().filter(|&&b| b == b'\n').count(), 1);
        let request: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            request,
            json!({"jsonrpc": "2.0", "id": 7, "method": "get \"tip\"", "params": [1, "a"]})
        );
    }

    fn reply(line: &str) -> Option<(u64, Result<Value, ChainError>)> {
        parse_reply(line.as_bytes()).map(|r| (r.id, r.result))
    }

    #[test]
    fn parses_results_and_server_errors() {
        assert_eq!(reply(r#"{"id":1,"result":5}"#), Some((1, Ok(json!(5)))));
        assert_eq!(
            reply(r#"{"id":2,"result":null,"error":null}"#),
            Some((2, Ok(Value::Null)))
        );
        assert_eq!(
            reply(r#"{"id":3,"error":null}"#),
            Some((3, Ok(Value::Null)))
        );
        assert_eq!(
            reply(r#"{"id":4,"error":{"code":-5,"message":"No such tx"}}"#),
            Some((
                4,
                Err(ChainError::Server {
                    code: -5,
                    message: "No such tx".into()
                })
            ))
        );
        assert_eq!(
            reply(r#"{"id":5,"error":{"code":1,"message":{"x":1}}}"#),
            Some((
                5,
                Err(ChainError::Server {
                    code: 1,
                    message: r#"{"x":1}"#.into()
                })
            ))
        );
        assert_eq!(
            reply(r#"{"id":6,"error":{"code":2}}"#),
            Some((
                6,
                Err(ChainError::Server {
                    code: 2,
                    message: String::new()
                })
            ))
        );
    }

    #[test]
    fn wrong_shapes_with_an_id_are_protocol_errors() {
        for line in [
            r#"{"id":1}"#,
            r#"{"id":1,"error":"boom"}"#,
            r#"{"id":1,"error":{"message":"no code"}}"#,
            r#"{"id":1,"error":{"code":"1","message":"string code"}}"#,
        ] {
            assert!(
                matches!(reply(line), Some((1, Err(ChainError::Protocol(_))))),
                "{}",
                line
            );
        }
    }

    #[test]
    fn lines_without_a_usable_id_are_dropped() {
        for line in [
            "not json",
            "[1,2]",
            r#"{"result":1}"#,
            r#"{"id":"1","result":1}"#,
            r#"{"id":-1,"result":1}"#,
            r#"{"method":"blockchain.headers.subscribe","params":[]}"#,
        ] {
            assert!(reply(line).is_none(), "{}", line);
        }
    }

    #[test]
    fn decode_handles_split_and_batched_lines() {
        let mut codec = LineJsonRpc::new();
        let mut replies = Vec::new();
        codec.decode(br#"{"id":1,"result":1}"#, &mut replies);
        assert!(replies.is_empty());
        codec.decode(b"\n{\"id\":2,\"res", &mut replies);
        codec.decode(
            b"ult\":2}\ngarbage\n{\"id\":3,\"result\":3}\n",
            &mut replies,
        );
        let ids: Vec<u64> = replies.iter().map(|r| r.id).collect();
        assert_eq!(ids, [1, 2, 3]);
        codec.decode(b"{\"id\":4", &mut replies);
        codec.reset();
        codec.decode(b"{\"id\":5,\"result\":5}\n", &mut replies);
        assert_eq!(replies.last().map(|r| r.id), Some(5));
        assert_eq!(replies.len(), 4);
    }
}
