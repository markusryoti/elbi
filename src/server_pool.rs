use futures::future::join_all;
use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct ServerPool {
    all_servers: Arc<Vec<String>>,
    passing_servers: Arc<Mutex<Vec<String>>>,
    current: Mutex<usize>,
}

impl ServerPool {
    pub fn new(servers: Vec<String>) -> Self {
        let cloned = servers.clone();
        ServerPool {
            all_servers: Arc::new(servers),
            passing_servers: Arc::new(Mutex::new(cloned)),
            current: Mutex::new(0),
        }
    }

    /// Does simple round robin
    ///
    pub fn get_connection(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let addr = if let Ok(mut curr) = self.current.lock() {
            if let Ok(servers) = self.passing_servers.lock() {
                *curr = (*curr + 1) % servers.len();
                servers.get(*curr).cloned()
            } else {
                None
            }
        } else {
            None
        };

        let addr = addr.ok_or(std::io::Error::other("mutex error"))?;
        let stream = TcpStream::connect(addr)?;

        Ok(stream)
    }

    pub fn start_background(&self, shutdown: CancellationToken) {
        let all_servers = Arc::clone(&self.all_servers);
        let ok_servers = Arc::clone(&self.passing_servers);
        tokio::spawn(async move { background_job(all_servers, ok_servers, shutdown).await });
    }
}

async fn background_job(
    servers: Arc<Vec<String>>,
    passing_servers: Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
) {
    let mut ticker = time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Stopping server healthchecks");
                return;
            }
            _ = ticker.tick() => {}
        }

        info!("Calling for server healthchecks");

        let ok_servers = run_healthchecks(&servers).await;

        if let Ok(mut ps) = passing_servers.lock() {
            ps.clear();
            ok_servers.into_iter().for_each(|s| ps.push(s));
        }

        info!("Passing servers updated: {:?}", passing_servers);
    }
}

async fn run_healthchecks(servers: &Arc<Vec<String>>) -> Vec<String> {
    let checks = servers.iter().map(async |s| {
        let r = backend_ok(s).await;
        (s, r)
    });

    let ok_servers: Vec<_> = join_all(checks)
        .await
        .into_iter()
        .filter_map(|(addr, res)| {
            let res = res.unwrap_or(false);
            if res { Some(addr.clone()) } else { None }
        })
        .collect();

    ok_servers
}

async fn backend_ok(addr: &str) -> Result<bool, reqwest::Error> {
    let res = reqwest::get(format!("http://{}/health", addr)).await?;
    let ok = res.status().is_success();
    Ok(ok)
}
