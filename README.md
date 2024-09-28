# solana-lite-indexer
A minimal indexer demonstration for Solana.

If you're looking for a production ready solana indexer, check out [solana-indexer](https://github.com/0xpapercut/solana-indexer)

## Setup
Install clickhouse and setup a user if you haven't yet done it. Create a database called `solana_lite_indexer`, which is the one we'll be using (you can modify the code if you wish to user another database).

Then, setup the following environment variables:
- `SOLANA_RPC`: Solana RPC endpoint;
- `SOLANA_WEBSOCKET`: Solana websocket endpoint. It should have the blockSubscribe method enabled, or otherwise this indexer won't work;
- `CLICKHOUSE_USER`: Your clickhouse user; and
- `CLICKHOUSE_PASSWORD`: Your clickhouse password.

Setup the database tables with `cargo run -- --setup-db`, and run the indexer with `cargo run`. Then, if you look at the clickhouse database, for example using Datagrip, you should see the rows being inserted in real time.

## Issues
This indexer does not deal with chain reorgs. When a reorg happens happens, we're supposed to backtrack from the head, while updating or deleting rows that have been inserted into the database but are no longer part of the chain, which is a little too involved for a toy project.
