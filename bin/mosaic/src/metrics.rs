//! Prometheus scrape endpoint.
//!
//! Runs on its own thread with its own tokio runtime, matching the RPC server:
//! the rest of mosaic is monoio, and the exporter needs tokio.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use metrics::gauge;
use metrics_exporter_prometheus::PrometheusBuilder;

/// Mirrors the `mosaic_nodeInfo` RPC. The peer handshake hard-matches all
/// three fields, so a cohort mismatch is a refused connection.
pub(crate) struct BuildInfo {
    pub(crate) peer_id_hex: String,
    pub(crate) protocol_version: u32,
    pub(crate) deployment_version: Option<String>,
    pub(crate) reduced_circuits: bool,
}

/// Starts the Prometheus endpoint on `bind_addr`. The thread runs for the life
/// of the process.
pub(crate) fn start(bind_addr: &str, build_info: BuildInfo) -> Result<()> {
    let addr: SocketAddr = bind_addr
        .parse()
        .with_context(|| format!("invalid metrics bind_addr `{bind_addr}`"))?;

    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "metrics endpoint is not on loopback — firewall it off from peers and the internet"
        );
    }

    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);

    std::thread::Builder::new()
        .name("metrics".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime for metrics endpoint");

            runtime.block_on(async move {
                // Report the bind result before parking, so a port clash fails
                // startup instead of silently leaving no endpoint.
                let install = PrometheusBuilder::new().with_http_listener(addr).install();
                let failed = install.is_err();
                let _ = ready_tx.send(install.map_err(|e| e.to_string()));
                if failed {
                    return;
                }

                gauge!(
                    "mosaic_build_info",
                    "peer_id" => build_info.peer_id_hex,
                    "protocol_version" => build_info.protocol_version.to_string(),
                    // Always labelled, so the series shape is the same for dev
                    // and coordinated deployments.
                    "deployment_version" => build_info.deployment_version.unwrap_or_else(|| "none".to_string()),
                    "reduced_circuits" => build_info.reduced_circuits.to_string(),
                )
                .set(1.0);

                std::future::pending::<()>().await;
            });
        })
        .context("failed to spawn metrics thread")?;

    ready_rx
        .recv()
        .context("metrics thread exited before reporting readiness")?
        .map_err(|e| anyhow::anyhow!("failed to install prometheus exporter: {e}"))?;

    tracing::info!(%addr, "metrics endpoint started");
    Ok(())
}
