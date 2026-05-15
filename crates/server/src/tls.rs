//! TLS plumbing for the kv server.
//!
//! Wraps an underlying transport (plaintext `TcpStream` or `monoio-rustls`
//! `TlsStream<TcpStream>`) behind a single [`BeyondStream`] enum so the HTTP
//! and RESP handlers stay generic across both modes. Both variants implement
//! [`monoio::io::AsyncReadRent`], [`monoio::io::AsyncWriteRent`], and the
//! marker [`monoio::io::Split`] trait, so the blanket `Splitable` impl
//! provides `into_split()` for free.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use anyhow::{Context, Result};
use monoio::BufResult;
use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, Split};
use monoio::net::TcpStream;
use monoio_rustls::ServerTlsStream;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

pub type TlsAcceptor = monoio_rustls::TlsAcceptor;
pub type TlsStream = ServerTlsStream<TcpStream>;

/// Transport stream used by the HTTP and RESP servers.
///
/// `BeyondStream::Plain` is a bare TCP connection. `BeyondStream::Tls` wraps
/// a completed TLS handshake. The split is established once at accept time,
/// after which the rest of the connection-handling code is variant-agnostic.
pub enum BeyondStream {
    Plain(TcpStream),
    Tls(Box<TlsStream>),
}

// SAFETY: Both inner variants implement `Split` — `TcpStream: Split` is
// provided by monoio, and `monoio_rustls::stream::Stream<IO, _>` implements
// `Split` whenever `IO: Split`. The enum forwards every read and write call
// to the active variant, so the underlying read/write independence holds.
unsafe impl Split for BeyondStream {}

impl AsyncReadRent for BeyondStream {
    #[inline]
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        match self {
            BeyondStream::Plain(s) => s.read(buf).await,
            BeyondStream::Tls(s) => s.read(buf).await,
        }
    }

    #[inline]
    async fn readv<T: IoVecBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        match self {
            BeyondStream::Plain(s) => s.readv(buf).await,
            BeyondStream::Tls(s) => s.readv(buf).await,
        }
    }
}

impl AsyncWriteRent for BeyondStream {
    #[inline]
    async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
        match self {
            BeyondStream::Plain(s) => s.write(buf).await,
            BeyondStream::Tls(s) => s.write(buf).await,
        }
    }

    #[inline]
    async fn writev<T: IoVecBuf>(&mut self, buf_vec: T) -> BufResult<usize, T> {
        match self {
            BeyondStream::Plain(s) => s.writev(buf_vec).await,
            BeyondStream::Tls(s) => s.writev(buf_vec).await,
        }
    }

    #[inline]
    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            BeyondStream::Plain(s) => s.flush().await,
            BeyondStream::Tls(s) => s.flush().await,
        }
    }

    #[inline]
    async fn shutdown(&mut self) -> std::io::Result<()> {
        match self {
            BeyondStream::Plain(s) => s.shutdown().await,
            BeyondStream::Tls(s) => s.shutdown().await,
        }
    }
}

/// Build a `ServerConfig` that requires a client certificate signed by the
/// supplied CA bundle. The returned config is HTTP/1.1-only (kv does not
/// speak HTTP/2 today).
pub fn load_server_config(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<Arc<ServerConfig>> {
    // Ensure a process-wide crypto provider is installed before constructing
    // any `ServerConfig`. Idempotent: re-installing on a second call is a
    // no-op for our purposes.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Load the CA bundle into a RootCertStore.
    let mut roots = RootCertStore::empty();
    let ca_file =
        File::open(ca_path).with_context(|| format!("opening TLS CA bundle at {ca_path}"))?;
    let mut ca_reader = BufReader::new(ca_file);
    for cert in rustls_pemfile::certs(&mut ca_reader) {
        let cert = cert.with_context(|| format!("parsing CA cert in {ca_path}"))?;
        roots
            .add(cert)
            .with_context(|| format!("adding CA cert from {ca_path} to root store"))?;
    }
    if roots.is_empty() {
        anyhow::bail!("no CA certificates found in {ca_path}");
    }

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .context("building client certificate verifier")?;

    // Load the server certificate chain.
    let cert_file =
        File::open(cert_path).with_context(|| format!("opening TLS cert at {cert_path}"))?;
    let mut cert_reader = BufReader::new(cert_file);
    let cert_chain: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<std::result::Result<_, _>>()
        .with_context(|| format!("parsing server cert chain in {cert_path}"))?;
    if cert_chain.is_empty() {
        anyhow::bail!("no certificates found in {cert_path}");
    }

    // Load the server private key (accept any of the supported PEM key formats).
    let key_file =
        File::open(key_path).with_context(|| format!("opening TLS key at {key_path}"))?;
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .with_context(|| format!("parsing private key in {key_path}"))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, key)
        .context("building rustls server config")?;
    // kv only speaks HTTP/1.1; advertise it explicitly so peers don't try h2.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    Ok(Arc::new(config))
}
