use elbi::Server;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let server = Server::new();
    server.run()?;

    Ok(())
}
