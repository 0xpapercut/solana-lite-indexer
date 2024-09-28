use tokio;
use anyhow::anyhow;
use std::sync::Arc;
use futures_util::StreamExt;
use tokio::sync::watch;

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_client::rpc_config::{RpcBlockConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding};
use clickhouse::Client as ClickhouseClient;

use crate::transaction::parse_block;
use crate::table::{TableRows, BlocksRow};

const _BLOCKS_TABLE_CACHE_SIZE: usize = 10000;
const _BLOCKS_TABLE_QUERY_BATCH_SIZE: u64 = 1000;
const DEFAULT_MAX_RPC_REQUESTS_PER_SECOND: u32 = 60;

pub struct IndexerConfig {
    _max_rpc_requests_per_second: u32,
    commitment_config: CommitmentConfig,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            _max_rpc_requests_per_second: DEFAULT_MAX_RPC_REQUESTS_PER_SECOND,
            commitment_config: CommitmentConfig::confirmed()
        }
    }
}

pub struct Indexer {
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

    pub async fn start(&'static self, shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        let (slot_tx, mut slot_rx) = watch::channel(0 as u64);
        let block_subscribe = tokio::spawn(self.block_subscribe(slot_tx, shutdown_rx.clone()));

        slot_rx.changed().await?;
        let latest_slot = slot_rx.borrow().clone();
        let main_backtracking = tokio::spawn(self.backtrack_from_slot(latest_slot, None, Some(false), shutdown_rx));

        block_subscribe.await??;
        main_backtracking.await??;
        Ok(())
    }

    async fn backtrack_from_slot(&'static self, initial_slot: u64, cutoff_slot: Option<u64>, until_blockhash_match: Option<bool>, stop_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        // The reference block is defined so that we know it is present in the database, as we extend to extend from it
        let mut ref_block: BlocksRow = match self.query_slot_on_database(initial_slot).await? {
            Some(block) => block,
            None => return Err(anyhow!("Cannot backtrack with no reference block to start from within the database")),
        };
        println!("Set reference block {:#?}", ref_block);

        loop {
            if stop_rx.has_changed().unwrap() {
                break;
            }

            if !cutoff_slot.is_none() && ref_block.parent_slot < cutoff_slot.unwrap() {
                println!("Ending backtrack because of cutoff_slot");
            }

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

            // Fetch parent block from RPC
            let parent_block = self.get_block_from_rpc(ref_block.parent_slot).await.unwrap();
            // Process block and insert changes in the database
            let mut table_rows = TableRows::default();
            parse_block(&mut table_rows, &parent_block, ref_block.parent_slot)?;
            table_rows.insert_into_database(&self.clickhouse).await?;
            // self.process_block(ref_block.parent_slot, &parent_block).await?;
            // Update the reference block
            ref_block = BlocksRow::from_confirmed_block(ref_block.parent_slot, &parent_block)?;
            println!("New reference block {:#?}", ref_block);
        }
        Ok(())
    }

    pub async fn block_subscribe(&'static self, slot_tx: watch::Sender<u64>, mut shutdown_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
        let commitment = self.config.commitment_config;

        let config = RpcBlockSubscribeConfig {
            commitment: Some(commitment),
            encoding: Some(UiTransactionEncoding::Base64),
            max_supported_transaction_version: Some(0),
            show_rewards: Some(false),
            transaction_details: None,
        };
        let (mut notifications, unsubscribe) = self.pubsub.block_subscribe(RpcBlockSubscribeFilter::All, Some(config)).await.unwrap();

        loop {
            tokio::select! {
                Some(notification) = notifications.next() => {
                    let slot = notification.context.slot;
                    let block = notification.value.block.unwrap();
                    println!("Received block notification from slot {}", slot);
                    self.process_block(slot, &block).await?;
                    slot_tx.send(slot)?;
                },
                _ = shutdown_rx.changed() => break,
            }
        }
        println!("Unsubscribing from block_subscribe");
        unsubscribe().await;

        Ok(())
    }

    pub async fn process_block(&'static self, slot: u64, block: &UiConfirmedBlock) -> Result<(), anyhow::Error> {
        let mut table_rows = TableRows::default();
        parse_block(&mut table_rows, block, slot)?;
        table_rows.insert_into_database(&self.clickhouse).await?;
        println!("Inserted rows from slot {} into database", slot);

        Ok(())
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

    async fn query_slot_on_database(&'static self, slot: u64) -> Result<Option<BlocksRow>, anyhow::Error> {
        Ok(self.clickhouse.query("SELECT * FROM blocks WHERE slot = ?").bind(slot).fetch_optional::<BlocksRow>().await?)
    }
}
