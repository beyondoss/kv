use std::path::PathBuf;

#[derive(clap::Args, Debug, Clone)]
pub struct Config {
    #[arg(long, env = "KV_DATA_DIR", default_value = "/var/lib/beyond/kv")]
    pub data_dir: PathBuf,

    #[arg(long, env = "KV_RESP_PORT", default_value_t = 6379)]
    pub resp_port: u16,

    #[arg(long, env = "KV_HTTP_PORT", default_value_t = 4869)]
    pub http_port: u16,

    #[arg(long, env = "KV_THREADS")]
    pub threads: Option<usize>,

    #[arg(long, env = "KV_MEMORY_BYTES", default_value_t = 256 * 1024 * 1024)]
    pub memory_bytes: usize,

    /// Auto-reclaim: trigger reclaim on a namespace when its sealed file count
    /// exceeds this value. Set to 0 to disable automatic reclaim.
    #[arg(long, env = "KV_RECLAIM_SEALED_THRESHOLD", default_value_t = 4)]
    pub reclaim_sealed_threshold: usize,

    /// Seconds between auto-reclaim scans. Ignored when reclaim_sealed_threshold is 0.
    #[arg(long, env = "KV_RECLAIM_INTERVAL_SECS", default_value_t = 300)]
    pub reclaim_interval_secs: u64,

    /// Maximum concurrent connections accepted per worker shard. Excess connections
    /// are dropped immediately with a busy response.
    #[arg(long, env = "KV_MAX_CONNS_PER_SHARD", default_value_t = 10_000)]
    pub max_conns_per_shard: usize,

    /// Seconds of inactivity before an idle connection is closed.
    #[arg(long, env = "KV_IDLE_TIMEOUT_SECS", default_value_t = 60)]
    pub idle_timeout_secs: u64,

    /// Maximum value size in bytes accepted via HTTP PUT or RESP SET.
    /// Requests with a Content-Length or body exceeding this are rejected with 413 / ERR.
    #[arg(long, env = "KV_MAX_VALUE_BYTES", default_value_t = 64 * 1024 * 1024)]
    pub max_value_bytes: usize,

    /// Number of consecutive log-sync failures on any shard before /readyz returns 503.
    #[arg(long, env = "KV_READYZ_SYNC_FAILURE_THRESHOLD", default_value_t = 3)]
    pub readyz_sync_failure_threshold: u32,
}

impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.memory_bytes == 0 {
            anyhow::bail!("KV_MEMORY_BYTES must be > 0");
        }
        let threads = self.threads.unwrap_or(1).max(1);
        if self.memory_bytes < threads {
            anyhow::bail!(
                "KV_MEMORY_BYTES ({}) is less than the thread count ({}); \
                 each shard would receive 0 bytes of cache",
                self.memory_bytes,
                threads
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(clap::Parser)]
    struct TestCli {
        #[command(flatten)]
        config: Config,
    }

    fn parse(args: &[&str]) -> Result<Config, clap::Error> {
        TestCli::try_parse_from(std::iter::once("beyond-kv").chain(args.iter().copied()))
            .map(|t| t.config)
    }

    #[test]
    fn defaults_are_sensible() {
        let cfg = parse(&[]).unwrap();
        assert_eq!(cfg.resp_port, 6379);
        assert_eq!(cfg.http_port, 4869);
        assert_eq!(cfg.memory_bytes, 256 * 1024 * 1024);
        assert!(cfg.threads.is_none());
    }

    #[test]
    fn explicit_flags_override_defaults() {
        let cfg = parse(&[
            "--resp-port",
            "7000",
            "--http-port",
            "8000",
            "--threads",
            "4",
            "--memory-bytes",
            "134217728",
            "--data-dir",
            "/tmp/kv-test",
        ])
        .unwrap();
        assert_eq!(cfg.resp_port, 7000);
        assert_eq!(cfg.http_port, 8000);
        assert_eq!(cfg.threads, Some(4));
        assert_eq!(cfg.memory_bytes, 134217728);
        assert_eq!(cfg.data_dir.to_str().unwrap(), "/tmp/kv-test");
    }

    #[test]
    fn invalid_resp_port_is_rejected() {
        assert!(parse(&["--resp-port", "not-a-port"]).is_err());
    }

    #[test]
    fn invalid_threads_is_rejected() {
        assert!(parse(&["--threads", "abc"]).is_err());
    }

    #[test]
    fn invalid_memory_bytes_is_rejected() {
        assert!(parse(&["--memory-bytes", "abc"]).is_err());
    }

    #[test]
    fn threads_zero_is_accepted_by_parser() {
        // The parser accepts 0; the runtime decides how to handle it.
        let cfg = parse(&["--threads", "0"]).unwrap();
        assert_eq!(cfg.threads, Some(0));
    }

    // ── Config::validate() ────────────────────────────────────────────────────

    #[test]
    fn validate_accepts_defaults() {
        let cfg = parse(&[]).unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_memory_bytes() {
        let mut cfg = parse(&[]).unwrap();
        cfg.memory_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_memory_below_thread_count() {
        let mut cfg = parse(&["--threads", "8"]).unwrap();
        cfg.memory_bytes = 4;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_memory_equal_to_thread_count() {
        let mut cfg = parse(&["--threads", "4"]).unwrap();
        cfg.memory_bytes = 4;
        assert!(cfg.validate().is_ok());
    }
}
