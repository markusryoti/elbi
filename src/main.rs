use std::{
    net::{Shutdown, TcpListener, TcpStream},
    thread,
};

use tracing::{error, info};

fn get_connection(addr: &str) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let stream = TcpStream::connect(addr)?;
    Ok(stream)
}

fn handle_sock(mut client: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut backend = get_connection("127.0.0.1:8080")?;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    info!("Listener binded to a port");

    for stream in listener.incoming() {
        let stream = stream.inspect_err(|e| error!("{}", e)).unwrap();
        handle_sock(stream)?;
    }

    Ok(())
}
