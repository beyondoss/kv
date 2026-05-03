use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "beyond-kv", about = "Beyond KV server")]
pub struct Config {
    #[arg(long, env = "KV_DATA_DIR", default_value = "/var/lib/beyond-kv")]
    pub data_dir: PathBuf,

    #[arg(long, env = "KV_RESP_PORT", default_value_t = 6379)]
    pub resp_port: u16,

    #[arg(long, env = "KV_HTTP_PORT", default_value_t = 4869)]
    pub http_port: u16,

    #[arg(long, env = "KV_THREADS")]
    pub threads: Option<usize>,

    #[arg(long, env = "KV_MEMORY_BYTES", default_value_t = 256 * 1024 * 1024)]
    pub memory_bytes: usize,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
