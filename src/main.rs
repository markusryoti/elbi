use elbi::load_balancer::LoadBalancer;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let shutdown = CancellationToken::new();

    let mut sigterm = signal(SignalKind::terminate())?;
    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("Received SIGINT"),
            _ = sigterm.recv() => info!("Received SIGTERM"),
        }
        signal_token.cancel();
    });

    let backends: Vec<String> = vec!["127.0.0.1:8080".into(), "127.0.0.1:8081".into()];
    let lb = LoadBalancer::new("127.0.0.1:7878".into(), backends);

    tokio::task::spawn_blocking(move || lb.run(shutdown).map_err(|e| e.to_string())).await??;

    info!("Shutdown complete");

    Ok(())
}
