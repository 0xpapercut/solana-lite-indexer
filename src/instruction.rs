// use solana_transaction_status::parse_instruction::ParsedInstruction;
// use solana_sdk::inner_instruction::InnerInstruction;
use solana_transaction_status::UiCompiledInstruction;
use solana_sdk::instruction::CompiledInstruction;
use bs58;

pub trait Instruction: Send + Sync {
    fn data(&self) -> Vec<u8>;
    fn accounts(&self) -> Vec<u8>;
    fn program_id_index(&self) -> u8;
    fn stack_height(&self) -> u32;
}

impl Instruction for CompiledInstruction {
    fn data(&self) -> Vec<u8> {
        self.data.clone()
    }
    fn accounts(&self) -> Vec<u8> {
        self.accounts.clone()
    }
    fn program_id_index(&self) -> u8 {
        self.program_id_index
    }
    fn stack_height(&self) -> u32 {
        1
    }
}

impl Instruction for UiCompiledInstruction {
    fn data(&self) -> Vec<u8> {
        bs58::decode(&self.data).into_vec().unwrap()
    }
    fn accounts(&self) -> Vec<u8> {
        self.accounts.clone()
    }
    fn program_id_index(&self) -> u8 {
       self.program_id_index
    }
    fn stack_height(&self) -> u32 {
        self.stack_height.unwrap()
    }
}
