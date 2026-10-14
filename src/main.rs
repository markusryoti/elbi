use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

use tracing::{error, info};

const MAX_DATA: usize = 4096;
const BUF_SIZE: usize = 1024;

fn handle_sock(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes: Vec<u8> = vec![];

    let mut read_bytes = 0;
    while read_bytes < MAX_DATA {
        let mut buf = vec![0; BUF_SIZE];
        let n_bytes = stream.read(&mut buf)?;

        if n_bytes == 0 {
            break;
        }

        bytes.append(&mut buf[..n_bytes].to_vec());
        read_bytes += n_bytes;

        stream.write_all(&buf)?;
        info!("Current buf: {}", String::from_utf8(buf)?);
    }

    info!(num_bytes = read_bytes, "request read");

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
