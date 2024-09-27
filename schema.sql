-- BLOCKS

CREATE TABLE blocks
(
    slot UInt64,
    parent_slot UInt64,
    blockhash VARCHAR(88),
    previous_blockhash VARCHAR(88),
    block_time DateTime,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY slot;

-- TRANSACTIONS

CREATE TABLE transactions
(
    signature VARCHAR(88),
    transaction_index UInt64,
    slot UInt64,
    PROJECTION projection (SELECT * ORDER BY (slot, transaction_index))
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (slot, transaction_index);

-- TOKEN TRANSFERS

CREATE TABLE spl_token_transfers
(
    slot UInt64,
    transaction_index UInt64,
    instruction_index UInt64,
    source_address LowCardinality(VARCHAR(44)) CODEC(LZ4),
    destination_address LowCardinality(VARCHAR(44)) CODEC(LZ4),
    amount UInt64,
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (slot, transaction_index, instruction_index);
