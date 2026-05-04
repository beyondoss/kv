use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "beyond-kv", about = "Beyond KV server")]
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
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Result<Config, clap::Error> {
        Config::try_parse_from(["beyond-kv"].iter().chain(args).copied())
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
            "--resp-port", "7000",
            "--http-port", "8000",
            "--threads", "4",
            "--memory-bytes", "134217728",
            "--data-dir", "/tmp/kv-test",
        ]).unwrap();
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
}
