use clap::Parser;
use lighthouse_client::{protocol::Authentication, Error, Lighthouse, Result, TokioWebSocket, LIGHTHOUSE_URL};
use tracing::{info, info_span, Instrument};

async fn run(lh: Lighthouse<TokioWebSocket>) -> Result<()> {
    info!("Connected to the Lighthouse server");

    async {
        _ = lh.delete(&["test"]).await;
        _ = lh.mkdir(&["test"]).await; // TODO: No longer ignore once Beacon no longer 400s here
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Recreating test directory")).await?;

    async {
        lh.post(&["test", "a", "nested"], "Hello world!".to_string()).await?;
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Posting to test directory")).await?;
    
    async {
        _ = lh.create(&["test", "b"]).await; // TODO: No longer ignore once Beacon no longer 418s here
        lh.link(&["test", "a", "nested"], &["test", "b"]).await?;
        lh.put(&["test", "a", "nested"], "Another string".to_string()).await?;
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Linking to sibling resource")).await?;

    async {
        let result: String = lh.get(&["test", "b"]).await?.payload;
        info!(result = result);
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Getting linked resource")).await?;

    async {
        lh.link(&["test", "a", "nested"], &["test", "b"]).await?;
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Unlinking sibling resource")).await?;

    async {
        let result: String = lh.get(&["test", "b"]).await?.payload;
        info!(result = result);
        info!(tree = %lh.list(&["test"]).await?.payload);
        Ok::<_, Error>(())
    }.instrument(info_span!("Getting unlinked resource")).await?;

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
