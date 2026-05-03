use std::rc::Rc;

use beyond_kv_engine::store::{ShardStore, DEFAULT_NS};
use beyond_kv_proto::command::Command;
use beyond_resp::{RespCodec, Value};
use monoio::net::{TcpListener, TcpStream};
use monoio_codec::Framed;

use crate::dispatch::dispatch;

pub struct ConnState {
    pub ns: &'static str,
    pub resp_version: u8,
    pub quit: bool,
}

impl Default for ConnState {
    fn default() -> Self {
        Self { ns: DEFAULT_NS, resp_version: 2, quit: false }
    }
}

pub async fn serve(store: Rc<ShardStore>, port: u16) {
    let addr = format!("0.0.0.0:{port}");
    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind {addr}: {e}");
            return;
        }
    };
    tracing::info!("RESP listening on {addr}");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                tracing::debug!(%peer, "accepted RESP connection");
                let store = store.clone();
                monoio::spawn(async move {
                    handle_conn(stream, store).await;
                });
            }
            Err(e) => {
                tracing::error!("accept error: {e}");
            }
        }
    }
}

async fn handle_conn(stream: TcpStream, store: Rc<ShardStore>) {
    let mut framed = Framed::new(stream, RespCodec::resp2());
    let mut state = ConnState::default();

    loop {
        use monoio::io::sink::Sink;
        use monoio::io::stream::Stream;

        let value = match framed.next().await {
            Some(Ok(v)) => v,
            Some(Err(e)) => {
                tracing::debug!("decode error: {e}");
                break;
            }
            None => break,
        };

        // HELLO needs codec version switch before we respond
        let is_hello = matches!(value, Value::Array(ref v) if v.first()
            .and_then(|v| if let Value::BulkString(b) = v { Some(b) } else { None })
            .map(|b| b.eq_ignore_ascii_case(b"HELLO"))
            .unwrap_or(false));

        let response = match Command::parse(value) {
            Ok(cmd) => {
                let resp = dispatch(cmd, &store, &mut state);
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
