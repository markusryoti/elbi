use futures::future::join_all;
use std::{
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tokio::time::{self, MissedTickBehavior};
use tracing::{Level, error, info, span};
use uuid::Uuid;

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
            let id = Uuid::new_v4().to_string();
            let span = span!(Level::INFO, "client_request", corr_id = id);
            let _guard = span.enter();

            info!("Grabbing a connection for client request");

            let stream = stream.inspect_err(|e| error!("{}", e))?;
            let backend = self.server_pool.get_connection()?;

            info!(
                addr = backend.peer_addr()?.to_string(),
                "Got connection for handling client request",
            );

            handle_sock(stream, backend)?;
        }

        Ok(())
    }
}

struct ServerPool {
    all_servers: Arc<Vec<String>>,
    passing_servers: Arc<Mutex<Vec<String>>>,
    current: Mutex<usize>,
}

impl ServerPool {
    fn new(servers: Vec<String>) -> Self {
        let cloned = servers.clone();
        ServerPool {
            all_servers: Arc::new(servers),
            passing_servers: Arc::new(Mutex::new(cloned)),
            current: Mutex::new(0),
        }
    }

    /// Does simple round robin
    ///
    fn get_connection(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
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

    fn start_background(&self) {
        let all_servers = Arc::clone(&self.all_servers);
        let ok_servers = Arc::clone(&self.passing_servers);
        tokio::spawn(async move { background_job(all_servers, ok_servers).await });
    }
}

async fn background_job(servers: Arc<Vec<String>>, passing_servers: Arc<Mutex<Vec<String>>>) {
    let mut ticker = time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;

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

fn handle_sock(
    mut client: TcpStream,
    mut backend: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Handling client request");

    let mut client_reader = client.try_clone()?;
    let mut backend_writer = backend.try_clone()?;

    let writer_span = span!(Level::INFO, "backend_writer");

    let client_to_backend = thread::spawn(move || -> std::io::Result<()> {
        writer_span.in_scope(|| {
            std::io::copy(&mut client_reader, &mut backend_writer)?;
            backend_writer.shutdown(Shutdown::Write)?;
            info!("Forwarded data from client to backend");
            Ok(())
        })
    });

    std::io::copy(&mut backend, &mut client)?;

    info!("Wrote data from backend to client");

    client.shutdown(Shutdown::Write)?;

    client_to_backend
        .join()
        .map_err(|_| std::io::Error::other("forwarding thread panic"))??;

    info!("Handled client request");

    Ok(())
}
