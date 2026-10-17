use socket2::{Domain, Protocol, Socket, Type};
use std::{
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, RecvTimeoutError},
    thread::{self, JoinHandle},
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
        let addr: SocketAddr = self.addr.parse().unwrap();

        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        socket.bind(&addr.into())?;
        socket.listen(16)?;

        let listener: TcpListener = socket.into();

        info!("Listener binded to an address");

        self.server_pool.start_background();

        for stream in listener.incoming() {
            let id = Uuid::new_v4().to_string();
            let span = span!(Level::INFO, "client_request", corr_id = id);
            let _guard = span.enter();

            let stream = match stream {
                Ok(s) => s,
                Err(e) => {
                    error!("connection failed: {e}");
                    continue;
                }
            };

            info!("Grabbing a connection for client request");

            let backend = match self.server_pool.get_connection() {
                Ok(backend) => backend,
                Err(e) => {
                    error!("connection failed: {e}");
                    continue;
                }
            };

            info!(
                addr = backend.peer_addr()?.to_string(),
                "Got connection for handling client request",
            );

            let deadline = Instant::now() + Duration::from_secs(5);

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
    let now = Instant::now();

    info!("Handling client request");

    let mut client_reader = client.try_clone()?;
    let mut backend_writer = backend.try_clone()?;

    let (c, b) = (client.try_clone()?, backend.try_clone()?);
    let (timeout_closer, close) = socket_timeout_closer(c, b, deadline);

    let writer_span = span!(Level::INFO, "backend_writer");
    let client_to_backend = thread::spawn(move || -> std::io::Result<()> {
        writer_span.in_scope(|| {
            std::io::copy(&mut client_reader, &mut backend_writer)?;
            backend_writer.shutdown(Shutdown::Write)?;
            info!("Forwarded data from client to backend");
            Ok(())
        })
    });

    let n_bytes = std::io::copy(&mut backend, &mut client)?;
    if n_bytes > 0 {
        info!(num_bytes = n_bytes, "Wrote data from backend to client");
    }

    client.shutdown(Shutdown::Write)?;

    client_to_backend
        .join()
        .map_err(|_| std::io::Error::other("forwarding thread panic"))??;

    close();
    let _ = timeout_closer.join();

    let elapsed_time = Instant::now() - now;
    info!(
        handling_time = format!("{} ms", elapsed_time.as_millis()),
        "Handled client request"
    );

    Ok(())
}

fn socket_timeout_closer(
    client: TcpStream,
    backend: TcpStream,
    deadline: Instant,
) -> (JoinHandle<()>, impl FnOnce()) {
    let (done_tx, done_rx) = mpsc::channel::<()>();

    let watchdog = thread::spawn(move || {
        if let Err(RecvTimeoutError::Timeout) =
            done_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            let _ = client.shutdown(Shutdown::Both);
            let _ = backend.shutdown(Shutdown::Both);
        }
    });

    let closer = || {
        drop(done_tx);
    };

    (watchdog, closer)
}
