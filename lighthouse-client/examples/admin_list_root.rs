use clap::Parser;
use lighthouse_client::{protocol::Authentication, Lighthouse, Result, TokioWebSocket, LIGHTHOUSE_URL};
use tracing::info;

async fn run(lh: Lighthouse<TokioWebSocket>) -> Result<()> {
    info!("Connected to the Lighthouse server");

    let tree = lh.list(&[]).await?.payload;
    info!("Got {}", tree);

    Ok(())
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
