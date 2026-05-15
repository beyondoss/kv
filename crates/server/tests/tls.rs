//! mTLS integration tests for the kv HTTP listener.
//!
//! Spins up a single-shard kv server on a monoio runtime in a background
//! thread (mirroring `tests/common/mod.rs`) but with a `TlsAcceptor` wired
//! into `http::serve_routed`. The test client uses `reqwest` over rustls.

use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair};
use tempfile::NamedTempFile;

pub struct CertBundle {
    pub ca_pem: String,
    pub server_pem: String,
    pub server_key_pem: String,
    pub client_pem: String,
    pub client_key_pem: String,
}

pub fn generate_test_certs() -> CertBundle {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let server_key = KeyPair::generate().unwrap();
    let mut srv_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    srv_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let server_cert = srv_params.signed_by(&server_key, &issuer).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut cli_params = CertificateParams::new(vec!["client".to_string()]).unwrap();
    cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = cli_params.signed_by(&client_key, &issuer).unwrap();

    CertBundle {
        ca_pem: ca_cert.pem(),
        server_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

fn write_temp(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f
}

fn mtls_client(certs: &CertBundle) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(certs.ca_pem.as_bytes()).unwrap();
    let combined = format!("{}{}", certs.client_pem, certs.client_key_pem);
    let identity = reqwest::Identity::from_pem(combined.as_bytes()).unwrap();
    reqwest::Client::builder()
        .add_root_certificate(ca)
        .identity(identity)
        .https_only(true)
        .build()
        .unwrap()
}

fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..2000 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("port {port} never became ready");
}

/// Holds resources that must outlive the server thread.
pub struct TlsTestServer {
    pub url: String,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
    _ca_file: NamedTempFile,
    _tmp_dir: tempfile::TempDir,
}

fn start_tls_server(certs: &CertBundle) -> TlsTestServer {
    let cert_file = write_temp(&certs.server_pem);
    let key_file = write_temp(&certs.server_key_pem);
    let ca_file = write_temp(&certs.ca_pem);

    let tls_config = beyond_kv::tls::load_server_config(
        cert_file.path().to_str().unwrap(),
        key_file.path().to_str().unwrap(),
        ca_file.path().to_str().unwrap(),
    )
    .expect("load_server_config");
    let acceptor = beyond_kv::tls::TlsAcceptor::from(tls_config);

    let tmp_dir = tempfile::TempDir::new().unwrap();
    let data_dir = tmp_dir.path().to_owned();

    let http_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_port = http_listener.local_addr().unwrap().port();

    let (http_tx, http_rx) =
        std::sync::mpsc::sync_channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);
    let (http_wakeup_read, http_wakeup_write) = std::os::unix::net::UnixStream::pair().unwrap();

    // Bridge the std listener to the worker channel; mirrors the production
    // accept loop but without per-shard routing (single shard).
    std::thread::spawn(move || {
        let mut w = http_wakeup_write;
        for stream in http_listener.incoming().flatten() {
            let peer = match stream.peer_addr() {
                Ok(p) => p,
                Err(_) => continue,
            };
            if http_tx.send((stream, peer)).is_err() {
                break;
            }
            if w.write_all(&[1u8]).is_err() {
                break;
            }
        }
    });

    std::thread::Builder::new()
        .name(format!("kv-tls-test-{http_port}"))
        .spawn(move || {
            monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                .enable_timer()
                .build()
                .expect("monoio runtime")
                .block_on(async move {
                    let n_shards = 1;
                    let (txs, wake_writes, mut rxs, mut wake_reads) =
                        beyond_kv::cross_shard::build_channels(n_shards);
                    let cross_shard_txs: Arc<[_]> = Arc::from(txs);
                    let cross_shard_wakeups: Arc<[_]> = Arc::from(wake_writes);

                    let shard_dir = data_dir.join("shard0");
                    std::fs::create_dir_all(&shard_dir).unwrap();
                    let store = Rc::new(
                        beyond_kv_engine::store::ShardStore::open(&shard_dir, 32 << 20)
                            .await
                            .expect("ShardStore::open"),
                    );
                    let cross_store = store.clone();
                    let cross_rx = rxs.remove(0);
                    let cross_wake = wake_reads.remove(0);
                    monoio::spawn(async move {
                        beyond_kv::cross_shard::serve(cross_store, cross_rx, cross_wake).await;
                    });

                    let sync_failures: Arc<[std::sync::atomic::AtomicU32]> = (0..n_shards)
                        .map(|_| std::sync::atomic::AtomicU32::new(0))
                        .collect::<Vec<_>>()
                        .into();
                    beyond_kv::http::serve_routed(
                        store,
                        http_rx,
                        http_wakeup_read,
                        10_000,
                        Duration::from_secs(60),
                        64 * 1024 * 1024,
                        0,
                        n_shards,
                        cross_shard_txs,
                        cross_shard_wakeups,
                        beyond_kv::metrics::Metrics::new(),
                        sync_failures,
                        3,
                        Some(acceptor),
                    )
                    .await;
                });
        })
        .expect("spawn tls server thread");

    wait_for_port(http_port);

    TlsTestServer {
        url: format!("https://localhost:{http_port}"),
        _cert_file: cert_file,
        _key_file: key_file,
        _ca_file: ca_file,
        _tmp_dir: tmp_dir,
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

/// Valid mTLS client — /livez returns 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tls_valid_client_gets_http_200() {
    let certs = generate_test_certs();
    let server = start_tls_server(&certs);

    let client = mtls_client(&certs);
    let res = client
        .get(format!("{}/livez", server.url))
        .send()
        .await
        .expect("request failed");

    assert_eq!(res.status(), 200);
}

/// No client certificate — TLS handshake fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tls_no_client_cert_rejected() {
    let certs = generate_test_certs();
    let server = start_tls_server(&certs);

    let ca = reqwest::Certificate::from_pem(certs.ca_pem.as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .https_only(true)
        .build()
        .unwrap();

    let err = client.get(format!("{}/livez", server.url)).send().await;
    assert!(err.is_err(), "expected handshake/connect failure");
}

/// Client cert signed by a different CA — server rejects it.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn tls_wrong_ca_rejected() {
    let server_certs = generate_test_certs();
    let rogue_certs = generate_test_certs();
    let server = start_tls_server(&server_certs);

    let client = mtls_client(&rogue_certs);
    let err = client.get(format!("{}/livez", server.url)).send().await;
    assert!(err.is_err(), "expected handshake/connect failure");
}
