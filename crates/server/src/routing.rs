//! Connection-routing helpers shared by the RESP and HTTP accept loops.
//!
//! The accept loop peeks the first bytes of an incoming connection to extract
//! a routing key, then hashes that key to pick a worker shard. When the key
//! cannot be extracted (slow client, unrecognized prefix), the caller falls
//! back to round-robin assignment.

use std::hash::Hasher as _;
use std::net::TcpStream;

use rustc_hash::FxHasher;

/// Hash `key` to a shard index in `[0, n)`.
pub fn shard_for_key(key: &[u8], n: usize) -> usize {
    let mut h = FxHasher::default();
    h.write(key);
    (h.finish() as usize) % n
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Backwards-compatible alias kept for any external callers; identical to `hex_val`.
pub fn peek_routing_key_bytes(b: u8) -> Option<u8> {
    hex_val(b)
}

/// Percent-decode bytes from a URL path segment to recover the raw routing key.
pub fn percent_decode_routing(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn peek_first_bytes(stream: &TcpStream, buf: &mut [u8; 4096]) -> usize {
    // Give slow-starting clients a 2ms window before falling back to round-robin.
    // Using set_read_timeout instead of toggling non-blocking avoids masking
    // WouldBlock as EOF.
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_millis(2)));
    let n = stream.peek(buf).unwrap_or(0);
    let _ = stream.set_read_timeout(None);
    n
}

/// Peeks the leading bytes of an incoming RESP connection to extract the routing key.
///
/// Because routing is based on the first command's key, **all commands on a
/// pipelined connection are routed to the same shard**. This is consistent with
/// Redis connection-pinning semantics: a client must not expect cross-shard
/// pipelining to work — each connection is bound to one shard for its lifetime.
pub fn peek_resp_key(stream: &TcpStream) -> Option<Vec<u8>> {
    let mut buf = [0u8; 4096];
    let n = peek_first_bytes(stream, &mut buf);
    if n == 0 {
        return None;
    }
    let buf = &buf[..n];

    if buf.first().copied() != Some(b'*') {
        return None;
    }
    // Find first \n
    let nl1 = buf.iter().position(|&b| b == b'\n')?;
    // Array count between buf[1..nl1-1] (strip \r)
    let count_end = if nl1 > 0 && buf[nl1 - 1] == b'\r' {
        nl1 - 1
    } else {
        nl1
    };
    let count_str = std::str::from_utf8(&buf[1..count_end]).ok()?;
    let count: usize = count_str.parse().ok()?;
    if count < 2 {
        return None;
    }

    // Skip first bulk-string element: $len\r\ncmd\r\n
    let mut i = nl1 + 1;
    if i >= buf.len() || buf[i] != b'$' {
        return None;
    }
    let len_start = i + 1;
    let nl2 = len_start + buf[len_start..].iter().position(|&b| b == b'\n')?;
    let len_end = if nl2 > 0 && buf[nl2 - 1] == b'\r' {
        nl2 - 1
    } else {
        nl2
    };
    let cmd_len: usize = std::str::from_utf8(&buf[len_start..len_end])
        .ok()?
        .parse()
        .ok()?;
    i = nl2 + 1 + cmd_len + 2; // skip cmd bytes + \r\n

    // Read second bulk-string element's value
    if i >= buf.len() || buf[i] != b'$' {
        return None;
    }
    let len_start = i + 1;
    let nl3 = len_start + buf[len_start..].iter().position(|&b| b == b'\n')?;
    let len_end = if nl3 > 0 && buf[nl3 - 1] == b'\r' {
        nl3 - 1
    } else {
        nl3
    };
    let key_len: usize = std::str::from_utf8(&buf[len_start..len_end])
        .ok()?
        .parse()
        .ok()?;
    let key_start = nl3 + 1;
    let key_end = key_start.checked_add(key_len)?;
    if key_end > buf.len() {
        return None;
    }
    Some(buf[key_start..key_end].to_vec())
}

/// Peek the leading bytes of an incoming HTTP connection, returning the key
/// extracted from `/values/{key}` segments when one is present.
pub fn peek_http_key(stream: &TcpStream) -> Option<Vec<u8>> {
    let mut buf = [0u8; 4096];
    let n = peek_first_bytes(stream, &mut buf);
    if n == 0 {
        return None;
    }
    let buf = &buf[..n];

    // Find end of request line
    let nl = buf.iter().position(|&b| b == b'\n')?;
    let line = &buf[..nl];
    // Parse: METHOD SP /path SP HTTP/1.1
    let mut parts = line.splitn(3, |&b| b == b' ');
    let _method = parts.next()?;
    let path = parts.next()?;
    let needle = b"/v1/kv/";
    let pos = path.windows(needle.len()).position(|w| w == needle)?;
    let after = &path[pos + needle.len()..];
    // Stop at query string or end of request line; slashes are part of the key.
    let key_end = after
        .iter()
        .position(|&b| b == b'?' || b == b' ')
        .unwrap_or(after.len());
    if key_end == 0 {
        return None;
    }
    let key_slice = &after[..key_end];
    // Strip /incr suffix so increment endpoint routes by the same key as GET/PUT.
    let key_bytes = key_slice
        .strip_suffix(b"/incr" as &[u8])
        .unwrap_or(key_slice);
    if key_bytes.is_empty() {
        return None;
    }
    // Percent-decode so the routing key matches the stored key.
    Some(percent_decode_routing(key_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};

    fn pair_with_payload(payload: &[u8]) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        client.write_all(payload).unwrap();
        client.flush().unwrap();
        // Drop client at end of test scope — keep alive via leak so the data
        // remains pendable in the kernel buffer for peek().
        std::mem::forget(client);
        // Give the kernel a moment to deliver the bytes.
        std::thread::sleep(std::time::Duration::from_millis(10));
        server_stream
    }

    #[test]
    fn peek_resp_key_extracts_second_bulk_string() {
        let s = pair_with_payload(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(peek_resp_key(&s), Some(b"foo".to_vec()));
    }

    #[test]
    fn peek_resp_key_returns_none_on_empty() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).unwrap();
        let (server_stream, _) = listener.accept().unwrap();
        // No bytes written — peek should return 0 and yield None.
        assert_eq!(peek_resp_key(&server_stream), None);
    }

    #[test]
    fn peek_http_key_extracts_values_segment() {
        let s = pair_with_payload(b"GET /v1/kv/mykey HTTP/1.1\r\n\r\n");
        assert_eq!(peek_http_key(&s), Some(b"mykey".to_vec()));
    }

    #[test]
    fn peek_http_key_returns_none_for_non_values_path() {
        let s = pair_with_payload(b"GET /livez HTTP/1.1\r\n\r\n");
        assert_eq!(peek_http_key(&s), None);
    }

    #[test]
    fn peek_http_key_returns_none_for_list_endpoint() {
        let s = pair_with_payload(b"GET /v1/kv?ns=0 HTTP/1.1\r\n\r\n");
        assert_eq!(peek_http_key(&s), None);
    }

    #[test]
    fn peek_http_key_extracts_key_for_incr_endpoint() {
        let s = pair_with_payload(b"GET /v1/kv/mykey/incr HTTP/1.1\r\n\r\n");
        assert_eq!(peek_http_key(&s), Some(b"mykey".to_vec()));
    }

    #[test]
    fn percent_decode_routing_handles_encoded_chars() {
        assert_eq!(percent_decode_routing(b"a%2Fb"), b"a/b".to_vec());
        assert_eq!(
            percent_decode_routing(b"hello%20world"),
            b"hello world".to_vec()
        );
        assert_eq!(percent_decode_routing(b"plain"), b"plain".to_vec());
    }

    #[test]
    fn peek_resp_key_count_one_returns_none() {
        // *1\r\n$4\r\nPING\r\n — single-element array has no key argument.
        let s = pair_with_payload(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(peek_resp_key(&s), None);
    }

    #[test]
    fn peek_resp_key_non_array_returns_none() {
        // Inline command (+PING\r\n) is not an array — must return None.
        let s = pair_with_payload(b"+PING\r\n");
        assert_eq!(peek_resp_key(&s), None);
    }

    #[test]
    fn peek_resp_key_key_beyond_buffer_returns_none() {
        // Claim a key_len larger than the peeked buffer so key_end > buf.len().
        // *2\r\n$3\r\nGET\r\n$9999\r\n<only a few bytes>\r\n
        let s = pair_with_payload(b"*2\r\n$3\r\nGET\r\n$9999\r\nshort\r\n");
        assert_eq!(peek_resp_key(&s), None);
    }

    #[test]
    fn percent_decode_routing_invalid_hex_passes_through() {
        // %GG is not valid hex — must be emitted verbatim as three bytes.
        assert_eq!(percent_decode_routing(b"%GG"), b"%GG".to_vec());
    }

    #[test]
    fn percent_decode_routing_incomplete_escape_passes_through() {
        // Trailing % with fewer than two hex digits left — must not panic.
        assert_eq!(percent_decode_routing(b"end%"), b"end%".to_vec());
        assert_eq!(percent_decode_routing(b"end%4"), b"end%4".to_vec());
    }
}
