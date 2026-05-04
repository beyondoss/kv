use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::mpsc;

use beyond_kv_engine::store::{ShardStore, DEFAULT_NS};
use beyond_kv_proto::command::Command;
use beyond_resp::{RespCodec, Value};
use monoio::io::AsyncReadRent;
use monoio::net::{TcpStream, UnixStream};
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
) {
    wakeup_read.set_nonblocking(true).expect("wakeup set_nonblocking");
    let mut wakeup = match UnixStream::from_std(wakeup_read) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to create wakeup stream: {e}");
            return;
        }
    };

    let mut buf = vec![0u8; 64];
    loop {
        let res;
        (res, buf) = wakeup.read(buf).await;
        match res {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        loop {
            match rx.try_recv() {
                Ok((stream, peer)) => {
                    if stream.set_nonblocking(true).is_err() {
                        tracing::error!(%peer, "set_nonblocking failed");
                        continue;
                    }
                    match TcpStream::from_std(stream) {
                        Ok(s) => {
                            tracing::debug!(%peer, "accepted RESP connection");
                            let store = store.clone();
                            monoio::spawn(async move { handle_conn(s, store).await });
                        }
                        Err(e) => tracing::error!(%peer, "TcpStream::from_std: {e}"),
                    }
                }
                Err(_) => break,
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
