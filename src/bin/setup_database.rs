use anyhow::Error;
use tokio;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use solana_client::rpc_response::RpcBlockUpdate;
use time::PrimitiveDateTime;

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::UiTransactionEncoding;

use clickhouse::{Client as ClickhouseClient, Row};


const DEFAULT_MAX_RPC_REQUESTS_PER_SECOND: u32 = 60;

#[derive(Row, Serialize, Deserialize, Debug)]
struct BlocksRow {
    slot: u64,
    parent_slot: u64,
    blockhash: String,
    previous_blockhash: String,
    block_time: u64,
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
    config: IndexerConfig
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

    pub async fn start(&mut self) {
        self.block_subscribe().await;
    }

    pub async fn block_subscribe(&mut self) -> Result<(), anyhow::Error> {
        let config = RpcBlockSubscribeConfig {
            commitment: Some(self.config.commitment_config),
            encoding: Some(UiTransactionEncoding::Base64),
            max_supported_transaction_version: Some(0),
            show_rewards: Some(false),
            transaction_details: None,
        };
        let (mut notifications, unsubscribe) = self.pubsub.block_subscribe(RpcBlockSubscribeFilter::All, Some(config)).await.unwrap();
        while let Some(notification) = notifications.next().await {
            println!("Received block notification from slot {}", notification.context.slot);
            self.process_block_update(notification.value).await?;
        }

        Ok(())
    }

    pub async fn process_block_update(&self, block_update: RpcBlockUpdate) -> Result<(), Error> {
        let block = block_update.block.unwrap();

        self.clickhouse.insert("blocks").unwrap().write(&BlocksRow {
            slot: block_update.slot,
            parent_slot: block.parent_slot,
            blockhash: block.blockhash,
            previous_blockhash: block.previous_blockhash,
            block_time: block.block_time.unwrap() as u64,
        }).await?;

        Ok(())
    }
}

// use fs;
#[derive(Serialize, Deserialize, Debug, Row)]
pub struct TablesRow {
    name: String,
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

    let tables: Vec<TablesRow> = clickhouse_client.query("SHOW TABLES").fetch_all().await.unwrap();
    for table in tables {
        clickhouse_client.query(&format!("DROP TABLE {}", table.name)).execute().await.unwrap();
    }


    let schema = fs::read_to_string("./schema.sql").unwrap();
    for statement in schema.split(';') {
        if !statement.trim().is_empty() {
            clickhouse_client.query(statement).execute().await.unwrap();
        }
    }

    // let mut indexer = Indexer::new(rpc_client, pubsub_client, clickhouse_client);
    // indexer.start().await;
}
