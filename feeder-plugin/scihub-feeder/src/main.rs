//! `scihub-feeder` — a single-upstream feeder sidecar.
//!
//! Upstream: Sci-Hub mirrors (operator-configured)
//!
//! Env:
//! - `META_FEEDER_HTTP_LISTEN` — listen addr (default `0.0.0.0:8080`)
//! - `META_FEEDER_STATE_DIR`   — per-plugin cache root (default `/data/meta-feeder`)
//! - `RUST_LOG`                — tracing filter (default `info`)

use std::net::SocketAddr;

use meta_feeder_sdk::{serve_feeders, FeederPlugin};
use scihub_feeder::scihub::ScihubPlugin;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let listen: SocketAddr = std::env::var("META_FEEDER_HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let state_dir =
        std::env::var("META_FEEDER_STATE_DIR").unwrap_or_else(|_| "/data/meta-feeder".to_string());

    let mut plugin = ScihubPlugin::new();
    if let Ok(raw) = std::env::var("SCIHUB_MIRRORS") {
        let mirrors: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !mirrors.is_empty() {
            plugin.set_mirrors(mirrors);
        }
    }

    let plugins: Vec<Box<dyn FeederPlugin>> = vec![Box::new(plugin)];
    serve_feeders(plugins, state_dir, listen).await
}
