use std::fs;
use clap::{Parser, Subcommand};
use tokio;
use tokio::signal;
use tokio::sync::watch;
use serde::{Serialize, Deserialize};

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use clickhouse::{Row, Client as ClickhouseClient};

pub mod indexer;
use indexer::Indexer;

pub mod table;
pub mod transaction;
pub mod instruction;

#[derive(Parser)]
#[command(name = "solana_lite_indexer")]
struct Cli {
    #[arg(long)]
    setup_db: bool,
    // #[command(subcommand)]
    // command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(name = "setup-db")]
    SetupDb,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    if cli.setup_db {
        let clickhouse_client = get_clickhouse_client();
        setup_db(clickhouse_client).await;
    } else {
        let clickhouse_client = get_clickhouse_client();
        let (rpc_client, pubsub_client) = get_rpc_and_pubsub().await;
        run_indexer(clickhouse_client, rpc_client, pubsub_client).await
    }
}

async fn run_indexer(clickhouse_client: ClickhouseClient, rpc_client: RpcClient, pubsub_client: PubsubClient) {
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

#[derive(Serialize, Deserialize, Debug, Row)]
pub struct TablesRow {
    name: String,
}

async fn setup_db(clickhouse_client: ClickhouseClient) {
    let tables: Vec<TablesRow> = clickhouse_client.query("SHOW TABLES").fetch_all().await.unwrap();
    for table in tables {
        clickhouse_client.query(&format!("DROP TABLE {}", table.name)).execute().await.unwrap();
    }
    println!("Database has been cleaned");

    let schema = fs::read_to_string("./schema.sql").unwrap();
    for statement in schema.split(';') {
        if !statement.trim().is_empty() {
            clickhouse_client.query(statement).execute().await.unwrap();
        }
    }
    println!("Tables sucessfully inserted");
}

async fn get_rpc_and_pubsub() -> (RpcClient, PubsubClient) {
    let rpc_url = std::env::var("SOLANA_HTTP").expect("SOLANA_HTTP environment variable not set");
    let rpc_client = RpcClient::new(rpc_url);
    let websocket_url = std::env::var("SOLANA_WEBSOCKET").expect("SOLANA_WEBSOCKET environment variable not set");
    let pubsub_client = PubsubClient::new(&websocket_url).await.unwrap();
    (rpc_client, pubsub_client)
}

fn get_clickhouse_client() -> ClickhouseClient {
    let clickhouse_user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER environment variable not set");
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD environment variable not set");
    let clickhouse_client = ClickhouseClient::default()
        .with_url("http://localhost:8123")
        .with_user(clickhouse_user)
        .with_password(clickhouse_password)
        .with_database("solana_lite_indexer")
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0");
    clickhouse_client
}
