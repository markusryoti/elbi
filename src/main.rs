use elbi::load_balancer::LoadBalancer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let backends: Vec<String> = vec!["127.0.0.1:8080".into(), "127.0.0.1:8081".into()];
    LoadBalancer::new("127.0.0.1:7878".into(), backends).run()?;

    Ok(())
}
