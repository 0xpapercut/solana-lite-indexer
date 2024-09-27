use anyhow::Result;
use futures_util::StreamExt;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockSubscribeConfig;
// use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::unbounded_channel;

use std::borrow::Borrow;

// use solana_client::rpc_client::RpcClient;
// use solana_client::pubsub_client::PubsubClient;
use solana_client::rpc_config::{RpcBlockConfig, RpcBlockSubscribeFilter, RpcTransactionLogsFilter, RpcTransactionLogsConfig};
use solana_client::rpc_response::{Response, RpcBlockUpdate};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{EncodedTransactionWithStatusMeta, TransactionDetails, UiCompiledInstruction, UiTransactionEncoding};
use clickhouse::Client;
// mod transaction;
// mod event;

use clickhouse::{Client as ClickhouseClient, Row};

struct IndexerConfig {
    max_requests_per_second: u32,
    rpc: RpcClient,
    pubsub: PubsubClient,
    clickhouse: ClickhouseClient,
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    let websocket_url = std::env::var("SOLANA_WEBSOCKET").expect("SOLANA_WEBSOCKET environment variable not set");
    let http_url = std::env::var("SOLANA_HTTP").expect("SOLANA_HTTP environment variable not set");

    let clickhouse_user = std::env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER environment variable not set");
    let clickhouse_password = std::env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD environment variable not set");
    let clickhouse_client = ClickhouseClient::default()
        .with_user(clickhouse_user)
        .with_password(clickhouse_password)
        .with_database("solana_lite_indexer");
    let clickhouse_client = Arc::new(Mutex::new(clickhouse_client));

    let pubsub_client = PubsubClient::new(&websocket_url).await.unwrap();
    // let pubsub_client = Arc::new(Mutex)
    let rpc_client = RpcClient::new(http_url);

    watch_subscriptions(websocket_url).await;
}

pub async fn watch_subscriptions(
    websocket_url: String
) -> Result<()> {

    // Subscription tasks will send a ready signal when they have subscribed.
    let (ready_sender, mut ready_receiver) = unbounded_channel::<()>();

    // Channel to receive unsubscribe channels (actually closures).
    // These receive a pair of `(Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>), &'static str)`,
    // where the first is a closure to call to unsubscribe, the second is the subscription name.
    let (unsubscribe_sender, mut unsubscribe_receiver) = unbounded_channel::<(_, &'static str)>();

    // The `PubsubClient` must be `Arc`ed to share it across tasks.
    let pubsub_client = Arc::new(PubsubClient::new(&websocket_url).await?);

    let mut join_handles = vec![];

    join_handles.push(("block", tokio::spawn({
        let ready_sender = ready_sender.clone();
        let unsubscribe_sender = unsubscribe_sender.clone();
        let pubsub_client = Arc::clone(&pubsub_client);
        async move {
            let config = Some(RpcBlockSubscribeConfig {
                commitment: Some(CommitmentConfig::confirmed()),
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::Full),
                show_rewards: Some(false),
                max_supported_transaction_version: Some(0),
            });
            let (mut block_notifications, block_unsubscribe) = pubsub_client.block_subscribe(solana_client::rpc_config::RpcBlockSubscribeFilter::All, config).await?;

            ready_sender.send(()).expect("channel");
            unsubscribe_sender.send((block_unsubscribe, "block")).map_err(|e| format!("{}", e)).expect("channel");
            drop((ready_sender, unsubscribe_sender));

            while let Some(block_info) = block_notifications.next().await {
                println!("--- Received block notification {} ---", block_info.context.slot);
            }

            Ok::<_, anyhow::Error>(())
        }
    })));

    join_handles.push(("slot", tokio::spawn({
        // Clone things we need before moving their clones into the `async move` block.
        //
        // The subscriptions have to be made from the tasks that will receive the subscription messages,
        // because the subscription streams hold a reference to the `PubsubClient`.
        // Otherwise we would just subscribe on the main task and send the receivers out to other tasks.

        let ready_sender = ready_sender.clone();
        let unsubscribe_sender = unsubscribe_sender.clone();
        let pubsub_client = Arc::clone(&pubsub_client);
        async move {
            let (mut slot_notifications, slot_unsubscribe) =
                pubsub_client.slot_subscribe().await?;

            // With the subscription started,
            // send a signal back to the main task for synchronization.
            ready_sender.send(()).expect("channel");

            // Send the unsubscribe closure back to the main task.
            unsubscribe_sender.send((slot_unsubscribe, "slot"))
                .map_err(|e| format!("{}", e)).expect("channel");

            // Drop senders so that the channels can close.
            // The main task will receive until channels are closed.
            drop((ready_sender, unsubscribe_sender));

            // Do something with the subscribed messages.
            // This loop will end once the main task unsubscribes.
            while let Some(slot_info) = slot_notifications.next().await {
                println!("--- Received slot notification {}", slot_info.slot);
            }

            // This type hint is necessary to allow the `async move` block to use `?`.
            Ok::<_, anyhow::Error>(())
        }
    })));

    // join_handles.push(("root", tokio::spawn({
    //     let ready_sender = ready_sender.clone();
    //     let unsubscribe_sender = unsubscribe_sender.clone();
    //     let pubsub_client = Arc::clone(&pubsub_client);
    //     async move {
    //         let (mut root_notifications, root_unsubscribe) =
    //             pubsub_client.root_subscribe().await?;

    //         ready_sender.send(()).expect("channel");
    //         unsubscribe_sender.send((root_unsubscribe, "root"))
    //             .map_err(|e| format!("{}", e)).expect("channel");
    //         drop((ready_sender, unsubscribe_sender));

    //         while let Some(root) = root_notifications.next().await {
    //             println!("------------------------------------------------------------");
    //             println!("root pubsub result: {:?}", root);
    //         }

    //         Ok::<_, anyhow::Error>(())
    //     }
    // })));

    // Drop these senders so that the channels can close
    // and their receivers return `None` below.
    drop(ready_sender);
    drop(unsubscribe_sender);

    // Wait until all subscribers are ready before proceeding with application logic.
    while let Some(_) = ready_receiver.recv().await { }

    // Do application logic here.

    // Wait for input or some application-specific shutdown condition.
    tokio::io::stdin().read_u8().await?;

    // Unsubscribe from everything, which will shutdown all the tasks.
    while let Some((unsubscribe, name)) = unsubscribe_receiver.recv().await {
        println!("unsubscribing from {}", name);
        unsubscribe().await
    }

    // Wait for the tasks.
    for (name, handle) in join_handles {
        println!("waiting on task {}", name);
        if let Ok(Err(e)) = handle.await {
            println!("task {} failed: {}", name, e);
        }
    }

    Ok(())
}


// pub async fn watch_subscriptions(
//     websocket_url: String
// ) -> Result<()> {
//     loop {
//             // Subscription tasks will send a ready signal when they have subscribed.
//     let (ready_sender, mut ready_receiver) = unbounded_channel::<()>();

//     // Channel to receive unsubscribe channels (actually closures).
//     let (unsubscribe_sender, mut unsubscribe_receiver) = unbounded_channel::<(_, &'static str)>();

//         // The `PubsubClient` must be `Arc`ed to share it across tasks.
//         let pubsub_client = match PubsubClient::new(&websocket_url).await {
//             Ok(client) => Arc::new(client),
//             Err(e) => {
//                 eprintln!("Failed to connect to PubsubClient: {}", e);
//                 tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
//                 continue;
//             }
//         };

//         let mut join_handles = vec![];

//         join_handles.push(("block", tokio::spawn({
//             let ready_sender = ready_sender.clone();
//             let unsubscribe_sender = unsubscribe_sender.clone();
//             let pubsub_client = Arc::clone(&pubsub_client);
//             async move {
//                 let config = Some(RpcBlockSubscribeConfig {
//                     commitment: Some(CommitmentConfig::confirmed()),
//                     encoding: Some(UiTransactionEncoding::Base64),
//                     transaction_details: Some(TransactionDetails::Full),
//                     show_rewards: Some(false),
//                     max_supported_transaction_version: Some(0),
//                 });
//                 let (mut block_notifications, block_unsubscribe) = pubsub_client.block_subscribe(solana_client::rpc_config::RpcBlockSubscribeFilter::All, config).await?;

//                 ready_sender.send(()).expect("channel");
//                 unsubscribe_sender.send((block_unsubscribe, "block")).map_err(|e| format!("{}", e)).expect("channel");
//                 drop((ready_sender, unsubscribe_sender));

//                 while let Some(block_info) = block_notifications.next().await {
//                     println!("--- Received block notification {} ---", block_info.context.slot);
//                 }

//                 Ok::<_, anyhow::Error>(())
//             }
//         })));

//         join_handles.push(("slot", tokio::spawn({
//             let ready_sender = ready_sender.clone();
//             let unsubscribe_sender = unsubscribe_sender.clone();
//             let pubsub_client = Arc::clone(&pubsub_client);
//             async move {
//                 let (mut slot_notifications, slot_unsubscribe) =
//                     pubsub_client.slot_subscribe().await?;

//                 ready_sender.send(()).expect("channel");
//                 unsubscribe_sender.send((slot_unsubscribe, "slot"))
//                     .map_err(|e| format!("{}", e)).expect("channel");
//                 drop((ready_sender, unsubscribe_sender));

//                 while let Some(slot_info) = slot_notifications.next().await {
//                     println!("--- Received slot notification {}", slot_info.slot);
//                 }

//                 Ok::<_, anyhow::Error>(())
//             }
//         })));

//         // Drop these senders so that the channels can close
//         // and their receivers return `None` below.
//         drop(ready_sender);
//         drop(unsubscribe_sender);

//         // Wait until all subscribers are ready before proceeding with application logic.
//         while let Some(_) = ready_receiver.recv().await { }

//         // Do application logic here.

//         // Wait for input or some application-specific shutdown condition.
//         tokio::io::stdin().read_u8().await?;

//         // Unsubscribe from everything, which will shutdown all the tasks.
//         while let Some((unsubscribe, name)) = unsubscribe_receiver.recv().await {
//             println!("unsubscribing from {}", name);
//             unsubscribe().await
//         }

//         // Wait for the tasks.
//         for (name, handle) in join_handles {
//             println!("waiting on task {}", name);
//             if let Ok(Err(e)) = handle.await {
//                 println!("task {} failed: {}", name, e);
//             }
//         }

//         // Reconnect logic
//         println!("Reconnecting to PubsubClient...");
//     }

//     Ok(())
// }

use futures_util::future;
use std::{time::{Duration, Instant}};
use tokio::{sync::{mpsc, Mutex, watch}, time};

struct SubscriptionManager {
    websocket_url: String,
    pubsub_client: Arc<PubsubClient>,
    // unsubscribe_sender: mpsc::UnboundedSender<(_, &'static str)>,
    block_last_message: Arc<Mutex<Instant>>,
    slot_last_message: Arc<Mutex<Instant>>,
}

impl SubscriptionManager {
    pub async fn new(websocket_url: String) -> Self {
        // let (unsubscribe_sender, _) = mpsc::unbounded_channel();
        Self {
            websocket_url: websocket_url.clone(),
            pubsub_client: Arc::new(PubsubClient::new(&websocket_url).await.unwrap()),
            // unsubscribe_sender,
            block_last_message: Arc::new(Mutex::new(Instant::now())),
            slot_last_message: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn start(&mut self, mut shutdown: watch::Receiver<()>) -> Result<()> {
        let mut join_handles = vec![];

        // Block subscription task.
        join_handles.push(self.subscribe_to_block().await);

        // Slot subscription task.
        join_handles.push(self.subscribe_to_slot().await);

        // Monitoring task for resubscription.
        let (restart_tx, restart_rx) = watch::channel(());
        // join_handles.push(tokio::spawn(self.monitor_subscriptions(restart_rx.clone())));

        // Wait for all tasks to complete or a shutdown signal.
        tokio::select! {
            _ = shutdown.changed() => {
                println!("Received shutdown signal, unsubscribing...");
                self.unsubscribe_all().await;
            }
            _ = futures_util::future::join_all(join_handles) => {
                println!("All tasks have completed.");
            }
        }

        Ok(())
    }

    async fn subscribe_to_block(&self) -> tokio::task::JoinHandle<Result<()>> {
        let pubsub_client = Arc::clone(&self.pubsub_client);
        // let unsubscribe_sender = self.unsubscribe_sender.clone();
        let block_last_message = Arc::clone(&self.block_last_message);
        tokio::spawn(async move {
            let (mut block_notifications, block_unsubscribe) = pubsub_client.block_subscribe(
                solana_client::rpc_config::RpcBlockSubscribeFilter::All,
                None,
            ).await?;
            // unsubscribe_sender.send((block_unsubscribe, "block")).expect("channel");

            while let Some(block_info) = block_notifications.next().await {
                *block_last_message.lock().await = Instant::now();
                println!("--- Received block notification {} ---", block_info.context.slot);
            }

            Ok(())
        })
    }

    async fn subscribe_to_slot(&self) -> tokio::task::JoinHandle<Result<()>> {
        let pubsub_client = Arc::clone(&self.pubsub_client);
        // let unsubscribe_sender = self.unsubscribe_sender.clone();
        let slot_last_message = Arc::clone(&self.slot_last_message);
        tokio::spawn(async move {
            let (mut slot_notifications, slot_unsubscribe) = pubsub_client.slot_subscribe().await?;
            // unsubscribe_sender.send((slot_unsubscribe, "slot")).expect("channel");

            while let Some(slot_info) = slot_notifications.next().await {
                *slot_last_message.lock().await = Instant::now();
                println!("--- Received slot notification {}", slot_info.slot);
            }

            Ok(())
        })
    }

    async fn monitor_subscriptions(&self, shutdown: watch::Receiver<()>) -> tokio::task::JoinHandle<Result<()>> {
    //     let monitor_duration = Duration::from_secs(30); // Adjust as needed
        tokio::spawn(async move {
    //         loop {
    //             let now = Instant::now();
    //             // let block_last = *self.block_last_message.lock().await;
    //             // le
    //             {
    //                 let lock = self.block_last_message.lock().await;
    //                 *lock
    //             };
    //             // let slot_last = *self.slot_last_message.lock().await;

    //             // if now.duration_since(block_last) > monitor_duration {
    //             //     println!("Resubscribing to block notifications due to timeout.");
    //             //     // shutdown.send(()).unwrap();
    //             //     break;
    //             // }

    //             // if now.duration_since(slot_last) > monitor_duration {
    //             //     println!("Resubscribing to slot notifications due to timeout.");
    //             //     // shutdown.send(()).unwrap();
    //             //     break;
    //             // }

    //             // Sleep for a short duration before checking again.
    //             tokio::time::sleep(Duration::from_secs(5)).await;
    //         }

            Ok(())
        })
    }

    async fn unsubscribe_all(&self) {
        // while let Ok((unsubscribe, name)) = self.unsubscribe_sender.recv().await {
        //     println!("Unsubscribing from {}", name);
        //     unsubscribe().await;
        // }
    }
}

// #[tokio::main]
// async fn main() -> Result<()> {
//     let websocket_url = "wss://example.com".to_string();
//     let (shutdown_tx, shutdown_rx) = watch::channel(());

//     let mut manager = SubscriptionManager::new(websocket_url).await;
//     manager.start(shutdown_rx).await
// }


