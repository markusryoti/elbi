use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
};

use tracing::{error, info};

const MAX_DATA: usize = 4096;
const BUF_SIZE: usize = 1024;

fn get_connection() -> Result<TcpStream, Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:8080")?;
    Ok(stream)
}

fn handle_sock(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes: Vec<u8> = vec![];

    let mut conn = get_connection()?;

    let mut read_bytes = 0;
    while read_bytes < MAX_DATA {
        let mut buf = vec![0; BUF_SIZE];
        let n_bytes = stream.read(&mut buf)?;

        if n_bytes == 0 {
            info!("No more bytes was read");
            break;
        }

        bytes.append(&mut buf[..n_bytes].to_vec());
        read_bytes += n_bytes;

        conn.write_all(&buf[..n_bytes]).unwrap();

        info!("Current buf: {}", String::from_utf8(buf)?);
    }

    conn.shutdown(Shutdown::Write)?;
    info!("Shutdown backend write");

    let mut res_buf = vec![];
    conn.read_to_end(&mut res_buf).unwrap();
    info!("Read result buf");

    stream.write_all(&res_buf)?;
    info!("Response buf:\n{}", String::from_utf8(res_buf)?);

    stream.shutdown(Shutdown::Write)?;

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
