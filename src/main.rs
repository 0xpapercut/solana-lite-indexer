use tokio;
use tokio::signal;
use tokio::sync::watch;

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use clickhouse::Client as ClickhouseClient;

mod indexer;
use indexer::Indexer;

mod instruction;

#[tokio::main]
async fn main() {
    let clickhouse_user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER environment variable not set");
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD environment variable not set");
    let clickhouse_client = ClickhouseClient::default()
        .with_url("http://localhost:8123")
        .with_user(clickhouse_user)
        .with_password(clickhouse_password)
        .with_database("solana_lite_indexer")
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");

    let websocket_url = std::env::var("SOLANA_WEBSOCKET").expect("SOLANA_WEBSOCKET environment variable not set");
    let rpc_url = std::env::var("SOLANA_HTTP").expect("SOLANA_HTTP environment variable not set");
    let pubsub_client = PubsubClient::new(&websocket_url).await.unwrap();
    let rpc_client = RpcClient::new(rpc_url);

    let indexer = Box::leak(Box::new(Indexer::new(rpc_client, pubsub_client, clickhouse_client)));
    let shutdown_tx = watch::Sender::new(());

    let shutdown_rx = shutdown_tx.subscribe();
    let start = tokio::spawn(async move {
        indexer.start(shutdown_rx).await.unwrap();
    });

    let mut shutdown_rx = shutdown_tx.subscribe();
    tokio::select! {
        _ = signal::ctrl_c() => { shutdown_tx.send(()).unwrap(); },
        _ = shutdown_rx.changed() => {},
    }
    println!("Shutting down...");
    start.await.unwrap();
}
