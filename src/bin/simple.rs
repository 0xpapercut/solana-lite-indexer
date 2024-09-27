use anyhow::Error;
use solana_client::rpc_config::RpcBlockConfig;
use solana_transaction_status::UiConfirmedBlock;
use tokio;
use anyhow::anyhow;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use std::sync::Mutex;
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

impl BlocksRow {
    pub fn from_confirmed_block(slot: u64, block: &UiConfirmedBlock) -> Result<Self, anyhow::Error> {
        Ok(Self {
            slot: slot,
            parent_slot: block.parent_slot,
            blockhash: block.blockhash.clone(),
            previous_blockhash: block.previous_blockhash.clone(),
            block_time: block.block_time.map(OffsetDateTime::from_unix_timestamp).unwrap()?,
        })
    }
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
}

impl Indexer {
    pub fn new(rpc: RpcClient, pubsub: PubsubClient, clickhouse: ClickhouseClient) -> Self {
        Self {
            rpc: Arc::new(rpc),
            pubsub: Arc::new(pubsub),
            clickhouse: Arc::new(clickhouse),
            config: IndexerConfig::default(),
        }
    }

    pub async fn start(&'static self, mut shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        // let (restart_tx, restart_rx) = watch::channel(());
        // let (shutdown_tx, shutdown_rx) = watch::channel(());
        // self.shutdown_tx = Some(Arc::new(shutdown_tx));

        // self.block_subscribe(shutdown_rx).await?;
        self.backtrack_from_slot(292167137, 0, Some(false), shutdown_rx).await.unwrap();
        Ok(())
    }

    async fn backtrack_from_slot(&'static self, ref_slot: u64, cutoff_slot: u64, until_blockhash_match: Option<bool>, mut stop_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        // The reference block is defined so that we know it is present in the database, as we extend to extend from it
        let mut ref_block: BlocksRow = match self.query_slot_on_database(ref_slot).await? {
            Some(block) => block,
            None => return Err(anyhow!("Cannot backtrack with no reference block to start from within the database")),
        };
        println!("Set reference block {:#?}", ref_block);

        loop {
            if stop_rx.has_changed().unwrap() {
                break;
            }

            if ref_block.parent_slot < cutoff_slot {
                println!("Ending backtrack because of cutoff_slot");
            }

            let parent_block = self.get_block_from_rpc(ref_block.parent_slot).await.unwrap();
            match self.query_slot_on_database(ref_block.parent_slot).await? {
                Some(parent_block_on_database) => {
                    println!("{}", parent_block_on_database.slot);
                    if parent_block_on_database.blockhash == ref_block.previous_blockhash {
                        if until_blockhash_match.unwrap_or(true) {
                            println!("Ending backtrack because of blockhash match on reference block {}", ref_block.slot);
                            break;
                        } else {
                            ref_block = parent_block_on_database;
                            println!("Continuing backtrack with updated reference slot {}", ref_block.slot);
                            continue;
                        }
                    }
                },
                None => ()
            }

            // Inserted block in the database and update reference block
            self.process_block(ref_block.parent_slot, &parent_block).await.unwrap();
            ref_block = BlocksRow::from_confirmed_block(ref_block.parent_slot, &parent_block)?;
            println!("New reference block {:#?}", ref_block);
        }
        Ok(())
    }

    pub async fn check_database_integrity(&'static self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    pub async fn block_subscribe(&'static self,  mut shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        let commitment = self.config.commitment_config;

        let config = RpcBlockSubscribeConfig {
            commitment: Some(commitment),
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
                    let slot = notification.context.slot;
                    let block = notification.value.block.unwrap();
                    println!("Received block notification from slot {}", slot);
                    self.process_block(slot, &block).await?;
                },
                _ = shutdown_rx.changed() => break,
            }
        }
        println!("Unsubscribing from block_subscribe");
        unsubscribe().await;

        // down_tx.send(());
        Ok(())
    }

    pub async fn process_block(&'static self, slot: u64, block: &UiConfirmedBlock) -> Result<(), Error> {
        let row = BlocksRow {
            slot,
            parent_slot: block.parent_slot,
            blockhash: block.blockhash.clone(),
            previous_blockhash: block.previous_blockhash.clone(),
            block_time: OffsetDateTime::from_unix_timestamp(block.block_time.unwrap())?,
        };
        let mut insert = self.clickhouse.insert("blocks")?;
        insert.write(&row).await?;
        insert.end().await?;
        println!("Inserted slot {} into the database", slot);

        Ok(())
    }

    pub async fn backtrack_from_slot_until_blockhash_match(slot: u64) {

    }

    pub async fn backtrack_from_slot_and_check_database_integrity(slot: u64) {

    }

    pub async fn get_block_from_rpc(&self, slot: u64) -> Result<UiConfirmedBlock, anyhow::Error> {
        let config = RpcBlockConfig {
            commitment: Some(self.config.commitment_config),
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: None,
            rewards: Some(false),
            max_supported_transaction_version: Some(0),
        };
        Ok(self.rpc.get_block_with_config(slot, config).await?)
    }

    async fn query_slot_on_database(&self, slot: u64) -> Result<Option<BlocksRow>, anyhow::Error> {
        let mut cursor = self.clickhouse.query("SELECT * FROM blocks WHERE slot = ?").bind(slot).fetch::<BlocksRow>()?;
        Ok(cursor.next().await?)
    }

    async fn delete_slot_on_database(&self, slot: u64) -> Result<(), anyhow::Error> {
        Ok(())
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
