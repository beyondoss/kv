use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use beyond_kv_engine::store::{ShardStore, DEFAULT_NS};
use beyond_kv_proto::command::Command;
use beyond_resp::{RespCodec, Value};
use monoio::net::TcpStream;
use monoio_codec::Framed;

use crate::dispatch::dispatch;

pub struct ConnState {
    pub ns: String,
    pub resp_version: u8,
    pub quit: bool,
}

impl Default for ConnState {
    fn default() -> Self {
        Self { ns: DEFAULT_NS.to_string(), resp_version: 2, quit: false }
    }
}

pub async fn serve(
    store: Rc<ShardStore>,
    rx: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    wakeup_read: StdUnixStream,
    max_conns: usize,
    idle_timeout: Duration,
) {
    crate::serve_loop(rx, wakeup_read, max_conns, "RESP", |s, _peer, guard| {
        let store = store.clone();
        monoio::spawn(async move {
            let _guard = guard;
            handle_conn(s, store, idle_timeout).await;
        });
    })
    .await;
}

async fn handle_conn(stream: TcpStream, store: Rc<ShardStore>, idle_timeout: Duration) {
    let mut framed = Framed::new(stream, RespCodec::resp2());
    let mut state = ConnState::default();

    loop {
        use monoio::io::sink::Sink;
        use monoio::io::stream::Stream;

        let value = match monoio::time::timeout(idle_timeout, framed.next()).await {
            Ok(Some(Ok(v))) => v,
            Ok(Some(Err(e))) => {
                tracing::debug!("decode error: {e}");
                break;
            }
            Ok(None) => break,
            Err(_elapsed) => {
                tracing::debug!("RESP connection idle timeout");
                break;
            }
        };

        // HELLO needs codec version switch before we respond
        let is_hello = matches!(value, Value::Array(ref v) if v.first()
            .and_then(|v| if let Value::BulkString(b) = v { Some(b) } else { None })
            .map(|b| b.eq_ignore_ascii_case(b"HELLO"))
            .unwrap_or(false));

        let response = match Command::parse(value) {
            Ok(cmd) => {
                let resp = dispatch(cmd, &store, &mut state).await;
                if is_hello {
                    framed.codec_mut().set_version(match state.resp_version {
                        3 => beyond_resp::Version::Resp3,
                        _ => beyond_resp::Version::Resp2,
                    });
                }
                resp
            }
            Err(e) => beyond_kv_proto::response::error("ERR", &e.to_string()),
        };

        if framed.send(response).await.is_err() {
            break;
        }
        if <_ as Sink<Value>>::flush(&mut framed).await.is_err() {
            break;
        }

        if state.quit {
            break;
        }
    }
}
