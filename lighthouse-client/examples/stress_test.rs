use std::time::Duration;

use clap::Parser;
use lighthouse_client::{protocol::{Authentication, Frame}, Lighthouse, Result, TokioWebSocket, LIGHTHOUSE_URL};
use tokio::time::{self, Instant};
use tracing::info;

async fn run(lh: Lighthouse<TokioWebSocket>, delay_ms: Option<u64>) -> Result<()> {
    info!("Connected to the Lighthouse server");

    let mut last_second = Instant::now();
    let mut frames_per_second = 0;

    loop {
        lh.put_model(Frame::fill(rand::random())).await?;
        frames_per_second += 1;
        // Interestingly, the loop is vastly quicker when running no delay (6k
        // fps in debug, 20k fps in release mode) compared to running with a
        // delay of 0 ms (500 fps in debug, 700 fps in release mode).
        if let Some(delay_ms) = delay_ms {
            time::sleep(Duration::from_millis(delay_ms)).await;
        }
        let now = Instant::now();
        if now.duration_since(last_second).as_millis() >= 1000 {
            info!(frames_per_second);
            frames_per_second = 0;
            last_second = now;
        }
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
    /// The delay in ms between successive requests.
    #[arg(short, long)]
    delay_ms: Option<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    _ = dotenvy::dotenv();

    let args = Args::parse();
    let auth = Authentication::new(&args.username, &args.token);
    let lh = Lighthouse::connect_with_tokio_to(&args.url, auth).await?;

    run(lh, args.delay_ms).await
}
