use std::{
    net::{Shutdown, TcpListener, TcpStream},
    sync::Mutex,
    thread,
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

        for stream in listener.incoming() {
            let stream = stream.inspect_err(|e| error!("{}", e)).unwrap();
            let backend = self.server_pool.get_connection()?;
            handle_sock(stream, backend)?;
        }

        Ok(())
    }
}

struct ServerPool {
    servers: Vec<String>,
    current: Mutex<usize>,
}

impl ServerPool {
    fn new(servers: Vec<String>) -> Self {
        ServerPool {
            servers,
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
