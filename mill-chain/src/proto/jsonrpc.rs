//! The JSON-RPC 1.0 envelope Bitcoin Core's HTTP server speaks, with batching.
//!
//! Error objects become [`ChainError::Server`] with their code and message; codes are not
//! interpreted here.

use crate::ChainError;
use serde_json::{Map, Value};

/// Appends one request.
pub(crate) fn encode_request(
    out: &mut Vec<u8>,
    id: u64,
    method: &str,
    params: &Value,
) -> Result<(), ChainError> {
    out.extend_from_slice(br#"{"jsonrpc":"1.0","id":"#);
    out.extend_from_slice(id.to_string().as_bytes());
    out.extend_from_slice(br#","method":"#);
    serde_json::to_writer(&mut *out, method).map_err(encode_error)?;
    out.extend_from_slice(br#","params":"#);
    serde_json::to_writer(&mut *out, params).map_err(encode_error)?;
    out.push(b'}');
    Ok(())
}

/// Appends a batch of `{"method", "params"}` objects, numbered from `first_id`.
pub(crate) fn encode_batch(
    out: &mut Vec<u8>,
    first_id: u64,
    requests: &[Value],
) -> Result<(), ChainError> {
    out.push(b'[');
    for (id, request) in (first_id..).zip(requests) {
        if id != first_id {
            out.push(b',');
        }
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| ChainError::Protocol("batch entry without a method".into()))?;
        let params = request.get("params").unwrap_or(&Value::Null);
        encode_request(out, id, method, params)?;
    }
    out.push(b']');
    Ok(())
}

fn encode_error(e: serde_json::Error) -> ChainError {
    ChainError::Protocol(format!("cannot encode request: {}", e))
}

/// The outcome of the reply to request `id`.
pub(crate) fn decode_reply(body: &[u8], id: u64) -> Result<Value, ChainError> {
    let reply: Value = serde_json::from_slice(body)
        .map_err(|e| ChainError::Protocol(format!("reply is not JSON: {}", e)))?;
    match reply.get("id") {
        Some(got) if got.as_u64() == Some(id) => reply_result(reply),
        // Core answers a request it could not parse with a null id.
        Some(Value::Null) | None if reply.get("error").is_some_and(Value::is_object) => {
            reply_result(reply)
        }
        got => Err(ChainError::Protocol(format!(
            "reply id {} does not match request id {}",
            got.unwrap_or(&Value::Null),
            id
        ))),
    }
}

/// Matches a batch reply to requests `first_id..first_id + n`, whatever order the server
/// answered in. Returns the reply objects in request order, with `null` for a request the
/// server did not answer.
pub(crate) fn decode_batch(body: &[u8], first_id: u64, n: usize) -> Result<Value, ChainError> {
    let reply: Value = serde_json::from_slice(body)
        .map_err(|e| ChainError::Protocol(format!("reply is not JSON: {}", e)))?;
    let replies = match reply {
        Value::Array(replies) => replies,
        // Core answers a batch it rejects whole with one error object.
        error @ Value::Object(_) => return reply_result(error).and(Err(not_a_batch())),
        _ => return Err(not_a_batch()),
    };
    let mut ordered = vec![Value::Null; n];
    for reply in replies {
        let slot = reply
            .get("id")
            .and_then(Value::as_u64)
            .and_then(|id| id.checked_sub(first_id))
            .and_then(|index| usize::try_from(index).ok())
            .filter(|&index| index < n)
            .ok_or_else(|| ChainError::Protocol("batch reply with an unknown id".into()))?;
        if !ordered[slot].is_null() {
            return Err(ChainError::Protocol("batch reply repeats an id".into()));
        }
        ordered[slot] = reply;
    }
    Ok(Value::Array(ordered))
}

fn not_a_batch() -> ChainError {
    ChainError::Protocol("batch reply is not an array".into())
}

/// Turns the array [`decode_batch`] returned back into one result per request.
pub(crate) fn split_batch_reply(
    replies: Value,
    n: usize,
) -> Result<Vec<Result<Value, ChainError>>, ChainError> {
    match replies {
        Value::Array(replies) if replies.len() == n => Ok(replies
            .into_iter()
            .map(|reply| match reply {
                Value::Null => Err(ChainError::Protocol("no reply in the batch".into())),
                reply => reply_result(reply),
            })
            .collect()),
        _ => Err(ChainError::Protocol(
            "batch reply has the wrong length".into(),
        )),
    }
}

/// The outcome one reply object carries. `"error": null` is success.
pub(crate) fn reply_result(reply: Value) -> Result<Value, ChainError> {
    let Value::Object(mut reply) = reply else {
        return Err(ChainError::Protocol("reply is not a JSON object".into()));
    };
    match reply.remove("error") {
        None | Some(Value::Null) => reply
            .remove("result")
            .ok_or_else(|| ChainError::Protocol("reply has neither result nor error".into())),
        Some(Value::Object(error)) => Err(server_error(&error)),
        Some(other) => Err(ChainError::Protocol(format!(
            "error is not an object: {}",
            other
        ))),
    }
}

fn server_error(error: &Map<String, Value>) -> ChainError {
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

    fn server(code: i64, message: &str) -> ChainError {
        ChainError::Server {
            code,
            message: message.into(),
        }
    }

    fn is_protocol<T: std::fmt::Debug>(result: &Result<T, ChainError>) -> bool {
        matches!(result, Err(ChainError::Protocol(_)))
    }

    #[test]
    fn encodes_a_1_0_request() {
        let mut out = b"x".to_vec();
        encode_request(&mut out, 7, "get\"tip", &json!([1, "a"])).unwrap();
        assert_eq!(
            out,
            br#"x{"jsonrpc":"1.0","id":7,"method":"get\"tip","params":[1,"a"]}"#
        );
    }

    #[test]
    fn encodes_a_batch_with_consecutive_ids() {
        let mut out = Vec::new();
        let requests = [
            json!({"method": "a", "params": [1]}),
            json!({"method": "b"}),
            json!({"method": "c", "params": {"k": true}}),
        ];
        encode_batch(&mut out, 41, &requests).unwrap();
        let batch: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            batch,
            json!([
                {"jsonrpc": "1.0", "id": 41, "method": "a", "params": [1]},
                {"jsonrpc": "1.0", "id": 42, "method": "b", "params": null},
                {"jsonrpc": "1.0", "id": 43, "method": "c", "params": {"k": true}},
            ])
        );
        let mut empty = Vec::new();
        encode_batch(&mut empty, 1, &[]).unwrap();
        assert_eq!(empty, b"[]");
        let mut bad = Vec::new();
        assert!(is_protocol(&encode_batch(
            &mut bad,
            1,
            &[json!({"params": []})]
        )));
        assert!(is_protocol(&encode_batch(
            &mut bad,
            1,
            &[json!({"method": 5})]
        )));
    }

    #[test]
    fn decodes_results_and_errors() {
        assert_eq!(
            decode_reply(br#"{"result":5,"error":null,"id":3}"#, 3),
            Ok(json!(5))
        );
        assert_eq!(
            decode_reply(br#"{"result":null,"id":3}"#, 3),
            Ok(Value::Null)
        );
        assert_eq!(
            decode_reply(
                br#"{"result":null,"error":{"code":-5,"message":"No such tx"},"id":3}"#,
                3
            ),
            Err(server(-5, "No such tx"))
        );
        assert_eq!(
            decode_reply(br#"{"error":{"code":-32601},"id":3}"#, 3),
            Err(server(-32601, ""))
        );
        assert_eq!(
            decode_reply(br#"{"error":{"code":1,"message":[1]},"id":3}"#, 3),
            Err(server(1, "[1]"))
        );
    }

    #[test]
    fn unparsed_requests_are_answered_with_a_null_id() {
        let body = br#"{"result":null,"error":{"code":-32700,"message":"Parse error"},"id":null}"#;
        assert_eq!(decode_reply(body, 9), Err(server(-32700, "Parse error")));
        let no_id = br#"{"error":{"code":-32700,"message":"Parse error"}}"#;
        assert_eq!(decode_reply(no_id, 9), Err(server(-32700, "Parse error")));
    }

    #[test]
    fn wrong_replies_are_protocol_errors() {
        for body in [
            &b"not json"[..],
            b"[1]",
            br#"{"result":1,"id":4}"#,
            br#"{"result":1,"id":"3"}"#,
            br#"{"result":1}"#,
            br#"{"result":1,"id":null}"#,
            br#"{"error":null,"id":null}"#,
            br#"{"id":3}"#,
            br#"{"error":"boom","id":3}"#,
            br#"{"error":{"message":"no code"},"id":3}"#,
            br#"{"error":{"code":1.5},"id":3}"#,
        ] {
            assert!(
                is_protocol(&decode_reply(body, 3)),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn batch_replies_are_matched_by_id_in_any_order() {
        let body = br#"[{"result":"c","error":null,"id":12},
                        {"result":null,"error":{"code":-5,"message":"nope"},"id":10},
                        {"result":"b","error":null,"id":11}]"#;
        let ordered = decode_batch(body, 10, 3).unwrap();
        assert_eq!(
            split_batch_reply(ordered, 3).unwrap(),
            [Err(server(-5, "nope")), Ok(json!("b")), Ok(json!("c"))]
        );
    }

    #[test]
    fn missing_batch_entries_fail_alone() {
        let ordered = decode_batch(br#"[{"result":2,"id":6}]"#, 5, 2).unwrap();
        let results = split_batch_reply(ordered, 2).unwrap();
        assert!(is_protocol(&results[0]));
        assert_eq!(results[1], Ok(json!(2)));
    }

    #[test]
    fn bad_batch_replies_fail_whole() {
        for body in [
            &b"nope"[..],
            b"5",
            br#"[{"result":1,"id":4}]"#,
            br#"[{"result":1,"id":7}]"#,
            br#"[{"result":1}]"#,
            br#"[{"result":1,"id":5},{"result":2,"id":5}]"#,
            br#"{"result":[],"error":null,"id":5}"#,
        ] {
            assert!(
                is_protocol(&decode_batch(body, 5, 2)),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        let rejected =
            br#"{"result":null,"error":{"code":-32700,"message":"Parse error"},"id":null}"#;
        assert_eq!(
            decode_batch(rejected, 5, 2),
            Err(server(-32700, "Parse error"))
        );
    }

    #[test]
    fn split_checks_the_shape() {
        assert!(is_protocol(&split_batch_reply(json!([1]), 2)));
        assert!(is_protocol(&split_batch_reply(json!({"a": 1}), 1)));
        assert_eq!(split_batch_reply(json!([]), 0).unwrap(), []);
    }

    #[test]
    fn reply_objects() {
        assert_eq!(reply_result(json!({"result": [1]})), Ok(json!([1])));
        assert!(is_protocol(&reply_result(json!([1]))));
        assert!(is_protocol(&reply_result(json!({}))));
        assert_eq!(
            reply_result(json!({"error": {"code": -18, "message": "no wallet"}})),
            Err(server(-18, "no wallet"))
        );
    }
}
