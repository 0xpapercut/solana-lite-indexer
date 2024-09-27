use anyhow::Error;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::v0::LoadedAddresses;
use solana_sdk::message::AccountKeys;
use solana_transaction_status::parse_accounts::ParsedAccount;
use solana_transaction_status::EncodedTransactionWithStatusMeta;
use solana_transaction_status::UiCompiledInstruction;
use solana_transaction_status::UiConfirmedBlock;
use solana_transaction_status::UiInstruction;
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

use spl_token::instruction::TokenInstruction;

use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedConfirmedBlock, UiTransactionEncoding};

use clickhouse::{Client as ClickhouseClient, Row};
use crate::instruction::Instruction;


// const DEFAULT_MAX_RPC_REQUESTS_PER_SECOND: u32 = 60;

#[derive(Row, Serialize, Deserialize, Debug)]
pub struct BlocksRow {
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

pub struct IndexerConfig {
    // max_rpc_requests_per_second: u32,
    commitment_config: CommitmentConfig,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            // max_rpc_requests_per_second: DEFAULT_MAX_RPC_REQUESTS_PER_SECOND,
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
        self.backtrack_from_slot(292167021, 0, Some(false), shutdown_rx).await.unwrap();
        Ok(())
    }

    async fn backtrack_from_slot(&'static self, ref_slot: u64, cutoff_slot: u64, until_blockhash_match: Option<bool>, stop_rx: watch::Receiver<()>) -> Result<(), anyhow::Error> {
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
        let mut insert_blocks = self.clickhouse.insert("blocks")?;
        insert_blocks.write(&row).await?;
        insert_blocks.end().await?;
        println!("Inserted slot {} into the database", slot);

        let mut rows = Vec::new();
        for (index, transaction) in block.transactions.as_ref().unwrap().iter().enumerate() {
            rows.extend(process_transaction(slot, transaction, index)?);
        }

        let mut insert_spl_token_transfers = self.clickhouse.insert("spl_token_transfers")?;
        for row in rows {
            match row {
                IndexerRow::SplTokenTransfer(spl_token_transfer_row) => {
                    insert_spl_token_transfers.write(&spl_token_transfer_row).await?;
                    println!("Inserting transfer row");
                }
                _ => (),
            }
        }
        insert_spl_token_transfers.end().await?;

        Ok(())
    }
    // pub async fn backtrack_from_slot_until_blockhash_match(slot: u64) {

    // }

    // pub async fn backtrack_from_slot_and_check_database_integrity(slot: u64) {

    // }

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
}

#[derive(Serialize, Deserialize, Debug, Row)]
pub struct SplTokenTransfersRow {
    slot: u64,
    transaction_index: u64,
    instruction_index: u64,
    source_address: String,
    destination_address: String,
    amount: u64,
}

pub fn process_transfer_instruction(instruction: &dyn Instruction, account_keys: &[String], slot: u64, transaction_index: usize, instruction_index: usize, amount: u64) -> Result<IndexerRow, anyhow::Error> {
    let source_address = account_keys[instruction.accounts()[0] as usize].clone();
    let destination_address = account_keys[instruction.accounts()[1] as usize].clone();

    let row = SplTokenTransfersRow {
        source_address,
        destination_address,
        slot,
        transaction_index: transaction_index as u64,
        instruction_index: instruction_index as u64,
        amount,
    };
    Ok(IndexerRow::SplTokenTransfer(row))
}

pub fn process_transaction(slot: u64, transaction_with_meta: &EncodedTransactionWithStatusMeta, transaction_index: usize) -> Result<Vec<IndexerRow>, anyhow::Error> {
    let mut rows = Vec::new();

    let transaction = &transaction_with_meta.transaction.decode().unwrap();
    let meta = transaction_with_meta.meta.as_ref().unwrap();

    let mut accounts_keys: Vec<String> = Vec::new();
    let static_account_keys = transaction.message.static_account_keys();
    let loaded_addresses = meta.loaded_addresses.as_ref().unwrap();
    accounts_keys.extend(static_account_keys.iter().map(|x| x.to_string()));
    accounts_keys.extend(loaded_addresses.writable.iter().cloned());
    accounts_keys.extend(loaded_addresses.readonly.iter().cloned());

    let instructions = transaction.message.instructions();
    let inner_instructions = meta.inner_instructions.as_ref().unwrap();

    let mut all_instructions: Vec<Arc<dyn Instruction>> = Vec::new();
    let mut inner_instructions_index = 0;
    for (i, main_instruction) in instructions.iter().enumerate() {
        all_instructions.push(Arc::new(main_instruction.clone()));
        if inner_instructions_index >= inner_instructions.len() || inner_instructions[inner_instructions_index].index != i as u8 {
            continue;
        }
        for inner_instruction in inner_instructions[inner_instructions_index].instructions.iter() {
            match inner_instruction {
                UiInstruction::Compiled(compiled) => all_instructions.push(Arc::new(compiled.clone())),
                _ => unimplemented!(),
            }
        }
        inner_instructions_index += 1;
    }

    for (i, instruction) in all_instructions.iter().enumerate() {
        let program_id = &accounts_keys[instruction.program_id_index() as usize];
        if *program_id == spl_token::id().to_string() {
            match TokenInstruction::unpack(instruction.data().as_slice()) {
                Ok(TokenInstruction::Transfer { amount }) => { rows.push(process_transfer_instruction(instruction.as_ref(), &accounts_keys, slot, transaction_index, i, amount)?) },
                Ok(TokenInstruction::TransferChecked { amount, decimals }) => { rows.push(process_transfer_instruction(instruction.as_ref(), &accounts_keys, slot, transaction_index, i, amount)?) },
                _ => (),
                Err(e) => (),
            }
        }
    }

    Ok(rows)
}

pub struct TransactionsRow;

pub enum IndexerRow {
    Block(BlocksRow),
    Transaction(TransactionsRow),
    SplTokenTransfer(SplTokenTransfersRow),
}
