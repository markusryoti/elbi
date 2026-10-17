use elbi::load_balancer::LoadBalancer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let backends: Vec<String> = vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string()];

    let lb = LoadBalancer::new(backends);
    lb.run()?;

    Ok(())
}
