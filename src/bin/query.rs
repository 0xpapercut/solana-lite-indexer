use anyhow::Error;
use tokio;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use solana_client::rpc_response::RpcBlockUpdate;
use std::time::SystemTime;
use chrono::{DateTime, Utc};
use clickhouse::serde::time::datetime;
// use time::PrimitiveDateTime;
use tokio::signal;
use tokio::sync::watch;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedConfirmedBlock, UiTransactionEncoding};

use clickhouse::{Client as ClickhouseClient, Row};


const DEFAULT_MAX_RPC_REQUESTS_PER_SECOND: u32 = 60;

#[derive(Row, Serialize, Deserialize, Debug)]
struct BlocksRow {
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    previous_blockhash: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    block_time: OffsetDateTime,
}

struct IndexerConfig {
    max_rpc_requests_per_second: u32,
    commitment_config: CommitmentConfig,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            max_rpc_requests_per_second: DEFAULT_MAX_RPC_REQUESTS_PER_SECOND,
            commitment_config: CommitmentConfig::confirmed()
        }
    }
}

struct Indexer {
    rpc: Arc<RpcClient>,
    pubsub: Arc<PubsubClient>,
    clickhouse: Arc<ClickhouseClient>,
    config: IndexerConfig,
    // down_rx: watch::Receiver<()>,
    // shutdown_tx: Option<Arc<watch::Sender<()>>>,
}

impl Indexer {
    pub fn new(rpc: RpcClient, pubsub: PubsubClient, clickhouse: ClickhouseClient) -> Self {
        Self {
            rpc: Arc::new(rpc),
            pubsub: Arc::new(pubsub),
            clickhouse: Arc::new(clickhouse),
            config: IndexerConfig::default(),
            // shutdown_tx: None,
            // restart_rx: Arc::new(data)
            // notify_restart: Arc::new(Notify::new())
            // block_unsubscribe: None,
        }
    }

    pub async fn start(&mut self, mut shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        // let (restart_tx, restart_rx) = watch::channel(());
        // let (shutdown_tx, shutdown_rx) = watch::channel(());
        // self.shutdown_tx = Some(Arc::new(shutdown_tx));

        self.block_subscribe(shutdown_rx).await?;
        Ok(())
    }

    // async fn monitor_block_subscription()

    pub async fn block_subscribe(&mut self, mut shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        let config = RpcBlockSubscribeConfig {
            commitment: Some(self.config.commitment_config),
            encoding: Some(UiTransactionEncoding::Base64),
            max_supported_transaction_version: Some(0),
            show_rewards: Some(false),
            transaction_details: None,
        };
        let (mut notifications, unsubscribe) = self.pubsub.block_subscribe(RpcBlockSubscribeFilter::All, Some(config)).await.unwrap();
        // let (down_tx, down_rx) = watch::channel(());
        // self.down_rx = down_rx;

        loop {
            tokio::select! {
                Some(notification) = notifications.next() => {
                    println!("Received block notification from slot {}", notification.context.slot);
                    self.process_block_update(notification.value).await?;
                },
                _ = shutdown_rx.changed() => break,
            }
        }
        println!("Unsubscribing from block_subscribe");
        unsubscribe().await;

        // down_tx.send(());
        Ok(())
    }

    pub async fn process_block_update(&self, block_update: RpcBlockUpdate) -> Result<(), Error> {
        let block = block_update.block.unwrap();

        let row = BlocksRow {
            slot: block_update.slot,
            parent_slot: block.parent_slot,
            blockhash: block.blockhash,
            previous_blockhash: block.previous_blockhash,
            block_time: OffsetDateTime::from_unix_timestamp(block.block_time.unwrap())?,
        };
        let mut insert = self.clickhouse.insert("blocks")?;
        insert.write(&row).await?;
        insert.end().await?;
        println!("Inserted slot {} into the database", block_update.slot);

        // let query = self.clickhouse.query("SELECT * FROM blocks WHERE slot = ?").bind(block.parent_slot).;

        Ok(())
    }

    pub async fn get_block(&self, slot: u64) -> Result<EncodedConfirmedBlock, anyhow::Error> {
        Ok(self.rpc.get_block_with_encoding(slot, UiTransactionEncoding::Base64).await?)
    }

    pub async fn insert_blocks_row(&self, row: BlocksRow) {
        let mut insert = self.clickhouse.insert("blocks").unwrap();
        insert.write(&row).await.unwrap();
    }

    // pub async fn is_down(&self) -> Result<(), anyhow::Error> {

    // }
}

#[tokio::main]
async fn main() {
    let clickhouse_user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER environment variable not set");
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD environment variable not set");
    let clickhouse_client = ClickhouseClient::default()
        .with_url("http://localhost:8123")
        .with_user(clickhouse_user)
        .with_password(clickhouse_password)
        .with_database("solana_lite_indexer");

    let websocket_url = std::env::var("SOLANA_WEBSOCKET").expect("SOLANA_WEBSOCKET environment variable not set");
    let rpc_url = std::env::var("SOLANA_HTTP").expect("SOLANA_HTTP environment variable not set");
    let pubsub_client = PubsubClient::new(&websocket_url).await.unwrap();
    let rpc_client = RpcClient::new(rpc_url);

    let query = clickhouse_client.query("SELECT * FROM blocks WHERE slot = ?").bind(0);
    let cursor = query.fetch::<BlocksRow>();
    let first_row = cursor.unwrap().next().await.unwrap();
    // row.
    println!("{:?}", first_row);

    // let mut indexer = Indexer::new(rpc_client, pubsub_client, clickhouse_client);
    // let shutdown_tx = watch::Sender::new(());

    // let mut shutdown_rx = shutdown_tx.subscribe();
    // let start = tokio::spawn(async move {
    //     indexer.start(shutdown_rx).await;
    // });

    // let mut shutdown_rx = shutdown_tx.subscribe();
    // tokio::select! {
    //     _ = signal::ctrl_c() => { shutdown_tx.send(()); },
    //     _ = shutdown_rx.changed() => {},
    // }
    // println!("Shutting down...");
    // start.await;

    // // indexer.is_down().await;
}
