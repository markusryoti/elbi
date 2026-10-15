use futures::future::join_all;
use tokio::time::{self, MissedTickBehavior};

use std::{
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use tracing::{error, info};

pub struct Server {
    server_pool: ServerPool,
}

impl Server {
    pub fn new(servers: Vec<String>) -> Self {
        Server {
            server_pool: ServerPool::new(servers),
        }
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
        info!("Listener binded to a port");

        self.server_pool.start_background();

        for stream in listener.incoming() {
            let stream = stream.inspect_err(|e| error!("{}", e)).unwrap();
            let backend = self.server_pool.get_connection()?;
            handle_sock(stream, backend)?;
        }

        Ok(())
    }
}

struct ServerPool {
    servers: Arc<Vec<String>>,
    current: Mutex<usize>,
}

impl ServerPool {
    fn new(servers: Vec<String>) -> Self {
        ServerPool {
            servers: Arc::new(servers),
            current: Mutex::new(0),
        }
    }

    /// Does simple round robin
    ///
    fn get_connection(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let addr = if let Ok(mut curr) = self.current.lock() {
            *curr = (*curr + 1) % self.servers.len();
            self.servers.get(*curr)
        } else {
            None
        };

        let addr = addr.ok_or(std::io::Error::other("mutex error"))?;
        let stream = TcpStream::connect(addr)?;

        Ok(stream)
    }

    fn start_background(&self) {
        let servers = Arc::clone(&self.servers);
        tokio::spawn(async move { background_job(servers).await });
    }
}

async fn background_job(servers: Arc<Vec<String>>) {
    let mut ticker = time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

        info!("Calling for server healthchecks");

        let failed_servers = run_healthchecks(&servers).await;

        info!("Failed servers: {:?}", failed_servers);
    }
}

async fn run_healthchecks(servers: &Arc<Vec<String>>) -> Vec<String> {
    let checks = servers.iter().map(async |s| {
        let r = backend_ok(s).await;
        (s, r)
    });

    let failed: Vec<_> = join_all(checks)
        .await
        .into_iter()
        .filter_map(|(addr, res)| {
            let res = res.unwrap_or(false);
            if !res { Some(addr.clone()) } else { None }
        })
        .collect();

    failed
}

async fn backend_ok(addr: &str) -> Result<bool, reqwest::Error> {
    let res = reqwest::get(format!("http://{}/health", addr)).await?;
    let ok = res.status().is_success();
    Ok(ok)
}

fn handle_sock(
    mut client: TcpStream,
    mut backend: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client_reader = client.try_clone()?;
    let mut backend_writer = backend.try_clone()?;

    let client_to_backend = thread::spawn(move || -> std::io::Result<()> {
        std::io::copy(&mut client_reader, &mut backend_writer)?;
        backend_writer.shutdown(Shutdown::Write)?;
        Ok(())
    });

    std::io::copy(&mut backend, &mut client)?;

    client.shutdown(Shutdown::Write)?;

    client_to_backend
        .join()
        .map_err(|_| std::io::Error::other("forwarding thread panic"))??;

    Ok(())
}
