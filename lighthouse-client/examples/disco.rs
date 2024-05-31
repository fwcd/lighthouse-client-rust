use clap::Parser;
use lighthouse_client::{protocol::{Authentication, Frame}, Lighthouse, Result, TokioWebSocket, LIGHTHOUSE_URL};
use tracing::info;
use tokio::time;
use std::time::Duration;

async fn run(lh: Lighthouse<TokioWebSocket>) -> Result<()> {
    info!("Connected to the Lighthouse server");

    loop {
        lh.put_model(Frame::fill(rand::random())).await?;
        info!("Sent frame");

        time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Parser)]
struct Args {
    /// The username.
    #[arg(short, long, env = "LIGHTHOUSE_USER")]
    username: String,
    /// The API token.
    #[arg(short, long, env = "LIGHTHOUSE_TOKEN")]
    token: String,
    /// The server URL.
    #[arg(long, env = "LIGHTHOUSE_URL", default_value = LIGHTHOUSE_URL)]
    url: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    _ = dotenvy::dotenv();

    let args = Args::parse();
    let auth = Authentication::new(&args.username, &args.token);
    let lh = Lighthouse::connect_with_tokio_to(&args.url, auth).await?;

    run(lh).await
}
