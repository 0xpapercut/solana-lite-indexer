use clickhouse::{Row, Client as ClickhouseClient};
use serde::{Serialize, Deserialize};
use time::OffsetDateTime;
use solana_transaction_status::UiConfirmedBlock;

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct BlocksRow {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash: String,
    pub previous_blockhash: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    pub block_time: OffsetDateTime,
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

#[derive(Row, Debug, Serialize, Deserialize, Clone)]
pub struct TransactionsRow {
    pub slot: u64,
    pub transaction_index: u64,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug, Row)]
pub struct SplTokenTransfersRow {
    pub slot: u64,
    pub transaction_index: u64,
    pub instruction_index: u64,
    pub source_address: String,
    pub destination_address: String,
    pub amount: u64,
}

pub struct TableRows {
    pub blocks: Vec<BlocksRow>,
    pub transactions: Vec<TransactionsRow>,
    pub spl_token_transfers: Vec<SplTokenTransfersRow>,
}

impl Default for TableRows {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            transactions: Vec::new(),
            spl_token_transfers: Vec::new(),
        }
    }
}

impl TableRows {
    pub fn extend(&mut self, other: Self) {
        self.blocks.extend(other.blocks);
        self.transactions.extend(other.transactions);
        self.spl_token_transfers.extend(other.spl_token_transfers);
    }

    pub async fn insert_into_database(&self, clickhouse: &ClickhouseClient) -> Result<(), anyhow::Error> {
        if !self.blocks.is_empty() {
            let mut insert_blocks = clickhouse.insert("blocks")?;
            for row in self.blocks.iter() {
                insert_blocks.write(row).await?;
            }
            insert_blocks.end().await?;
        }

        if !self.transactions.is_empty() {
            let mut insert_transactions = clickhouse.insert("transactions")?;
            for row in self.transactions.iter() {
                insert_transactions.write(row).await?;
            }
            insert_transactions.end().await?;
        }

        if !self.spl_token_transfers.is_empty() {
            let mut insert_spl_token_transfers = clickhouse.insert("spl_token_transfers")?;
            for row in self.spl_token_transfers.iter() {
                insert_spl_token_transfers.write(row).await?;
            }
            insert_spl_token_transfers.end().await?;
        }

        Ok(())
    }
}
