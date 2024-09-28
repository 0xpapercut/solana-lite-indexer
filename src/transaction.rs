use std::sync::Arc;
use time::OffsetDateTime;

use solana_transaction_status::{UiConfirmedBlock, EncodedTransactionWithStatusMeta, UiInstruction};
use spl_token::instruction::TokenInstruction;
use spl_token;

use crate::instruction::Instruction;
use crate::table::{TableRows, BlocksRow, TransactionsRow, SplTokenTransfersRow};

pub fn parse_block(table_rows: &mut TableRows, block: &UiConfirmedBlock, slot: u64) -> Result<(), anyhow::Error> {
    table_rows.blocks.push(BlocksRow {
        slot,
        parent_slot: block.parent_slot,
        blockhash: block.blockhash.clone(),
        previous_blockhash: block.previous_blockhash.clone(),
        block_time: OffsetDateTime::from_unix_timestamp(block.block_time.unwrap())?,
    });

    for (index, transaction) in block.transactions.as_ref().unwrap().iter().enumerate() {
        parse_transaction(table_rows, transaction, slot, index)?;
    }

    Ok(())
}

pub fn parse_transaction(table_rows: &mut TableRows, transaction_with_meta: &EncodedTransactionWithStatusMeta, slot: u64, transaction_index: usize) -> Result<(), anyhow::Error> {
    let transaction = &transaction_with_meta.transaction.decode().unwrap();
    let meta = transaction_with_meta.meta.as_ref().unwrap();

    table_rows.transactions.push(TransactionsRow {
        slot,
        transaction_index: transaction_index as u64,
        signature: transaction.signatures[0].to_string(),
    });

    let mut account_keys: Vec<String> = Vec::new();
    let static_account_keys = transaction.message.static_account_keys();
    let loaded_addresses = meta.loaded_addresses.as_ref().unwrap();
    account_keys.extend(static_account_keys.iter().map(|x| x.to_string()));
    account_keys.extend(loaded_addresses.writable.iter().cloned());
    account_keys.extend(loaded_addresses.readonly.iter().cloned());

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

    for (index, instruction) in all_instructions.iter().enumerate() {
        parse_instruction(table_rows, instruction.as_ref(), account_keys.as_ref(), slot, transaction_index, index)?;
    }

    Ok(())
}

pub fn parse_instruction(table_rows: &mut TableRows, instruction: &dyn Instruction, account_keys: &[String], slot: u64, transaction_index: usize, instruction_index: usize) -> Result<(), anyhow::Error> {
    let program_id = &account_keys[instruction.program_id_index() as usize];

    if *program_id == spl_token::id().to_string() {
        match TokenInstruction::unpack(instruction.data().as_slice())? {
            TokenInstruction::Transfer { amount } => { table_rows.spl_token_transfers.push(parse_spl_token_transfer_instruction(instruction, account_keys, slot, transaction_index, instruction_index, amount)?) },
            TokenInstruction::TransferChecked { amount, decimals: _ } => { table_rows.spl_token_transfers.push(parse_spl_token_transfer_instruction(instruction, account_keys, slot, transaction_index, instruction_index, amount)?) },
            _ => (),
        }
    }

    Ok(())
}

pub fn parse_spl_token_transfer_instruction(instruction: &dyn Instruction, account_keys: &[String], slot: u64, transaction_index: usize, instruction_index: usize, amount: u64) -> Result<SplTokenTransfersRow, anyhow::Error> {
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
    Ok(row)
}
