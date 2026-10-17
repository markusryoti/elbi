use std::{
    net::{Shutdown, TcpListener, TcpStream},
    sync::mpsc::{self, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};
use tracing::{Level, error, info, span};
use uuid::Uuid;

use crate::server_pool::ServerPool;

pub struct LoadBalancer {
    addr: String,
    server_pool: ServerPool,
}

impl LoadBalancer {
    pub fn new(addr: String, servers: Vec<String>) -> Self {
        LoadBalancer {
            addr,
            server_pool: ServerPool::new(servers),
        }
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).unwrap();
        info!("Listener binded to a port");

        self.server_pool.start_background();

        for stream in listener.incoming() {
            let id = Uuid::new_v4().to_string();
            let span = span!(Level::INFO, "client_request", corr_id = id);
            let _guard = span.enter();

            let deadline = Instant::now() + Duration::from_secs(5);

            info!("Grabbing a connection for client request");

            let stream = stream.inspect_err(|e| error!("{}", e))?;

            let backend = self.server_pool.get_connection()?;

            info!(
                addr = backend.peer_addr()?.to_string(),
                "Got connection for handling client request",
            );

            if let Err(e) = handle_sock(stream, backend, deadline) {
                error!("connection failed: {e}");
            }
        }

        Ok(())
    }
}

fn handle_sock(
    mut client: TcpStream,
    mut backend: TcpStream,
    deadline: Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Handling client request");

    let mut client_reader = client.try_clone()?;
    let mut backend_writer = backend.try_clone()?;

    let (done_tx, done_rx) = mpsc::channel::<()>();
    let (c, b) = (client.try_clone()?, backend.try_clone()?);

    let watchdog = thread::spawn(move || {
        match done_rx.recv_timeout(deadline - Instant::now()) {
            Err(RecvTimeoutError::Timeout) => {
                // deadline hit: kill both sockets
                let _ = c.shutdown(Shutdown::Both);
                let _ = b.shutdown(Shutdown::Both);
            }
            _ => {} // Disconnected (or a message) = finished normally
        }
    });

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

    drop(done_tx);
    let _ = watchdog.join();

    info!("Handled client request");

    Ok(())
}
