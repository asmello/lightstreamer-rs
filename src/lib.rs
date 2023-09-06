mod client;
mod connection;
mod notification;
mod session;
mod stream;
mod subscription;
mod url;
mod util;
mod watcher;

pub use client::Client;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ::url::Url;
    use tokio::time::sleep;
    use tracing_test::traced_test;

    use crate::session::SubscriptionMode;

    use super::*;

    #[tokio::test]
    #[traced_test]
    async fn create_session() {
        let url = Url::parse("http://localhost/").unwrap();
        let client = Client::builder(url)
            .adapter_set("WELCOME")
            .content_length(10_000)
            .build()
            .unwrap();

        let session = client.create_session().await.unwrap();
        let subscription1 = session
            .subscribe(
                "STOCKS",
                &["item1"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();
        let subscription2 = session
            .subscribe(
                "STOCKS",
                &["item2"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();

        let mut updates = subscription1.updates();
        tokio::spawn(async move {
            while let Some(update) = updates.recv().await {
                println!("sub1: {update:?}");
            }
            println!("sub1 finished");
        });

        let mut updates = subscription2.updates();
        tokio::spawn(async move {
            while let Some(update) = updates.recv().await {
                println!("sub2: {update:?}");
            }
            println!("sub2 finished");
        });

        sleep(Duration::from_secs(600)).await;

        subscription1.unsubscribe().await.unwrap();

        sleep(Duration::from_secs(5)).await;

        session.destroy().await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn close_stream() {
        let url = Url::parse("http://localhost/").unwrap();
        let client = Client::builder(url).adapter_set("WELCOME").build().unwrap();

        let session = client.create_session().await.unwrap();
        let subscription = session
            .subscribe(
                "STOCKS",
                &["item2"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();

        sleep(Duration::from_secs(10)).await;

        session.stream().borrow().close();

        sleep(Duration::from_secs(5)).await;

        session.recover().await.unwrap();

        sleep(Duration::from_secs(10)).await;

        subscription.unsubscribe().await.unwrap();

        sleep(Duration::from_secs(5)).await;

        session.destroy().await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn snapshots() {
        let url = Url::parse("http://localhost/").unwrap();
        let client = Client::builder(url).adapter_set("WELCOME").build().unwrap();

        let session = client.create_session().await.unwrap();
        let subscription = session
            .subscribe(
                "STOCKS",
                &["item2"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();

        let mut snapshots = subscription.snapshots();
        tokio::spawn(async move {
            loop {
                let snapshot = snapshots.recv().await.unwrap();
                println!("{snapshot:?}");
            }
        });

        sleep(Duration::from_secs(10)).await;

        subscription.unsubscribe().await.unwrap();

        sleep(Duration::from_secs(5)).await;

        session.destroy().await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn create_websocket_session() {
        let url = Url::parse("ws://localhost/").unwrap();
        let client = Client::builder(url).adapter_set("WELCOME").build().unwrap();

        let session = client.create_session().await.unwrap();

        let subscription1 = session
            .subscribe(
                "STOCKS",
                &["item1"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();
        let subscription2 = session
            .subscribe(
                "STOCKS",
                &["item2"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();

        let mut updates = subscription1.updates();
        tokio::spawn(async move {
            while let Some(update) = updates.recv().await {
                println!("sub1: {update:?}");
            }
            println!("sub1 finished");
        });

        let mut updates = subscription2.updates();
        tokio::spawn(async move {
            while let Some(update) = updates.recv().await {
                println!("sub2: {update:?}");
            }
            println!("sub2 finished");
        });

        sleep(Duration::from_secs(10)).await;

        subscription1.unsubscribe().await.unwrap();

        sleep(Duration::from_secs(5)).await;

        session.destroy().await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    #[traced_test]
    async fn close_websocket_stream() {
        let url = Url::parse("ws://localhost/").unwrap();
        let client = Client::builder(url).adapter_set("WELCOME").build().unwrap();

        let session = client.create_session().await.unwrap();
        let subscription = session
            .subscribe(
                "STOCKS",
                &["item2"],
                &["stock_name", "time", "last_price"],
                SubscriptionMode::Merge,
            )
            .await
            .unwrap();

        sleep(Duration::from_secs(10)).await;

        session.stream().borrow().close();

        sleep(Duration::from_secs(5)).await;

        session.recover().await.unwrap();

        sleep(Duration::from_secs(10)).await;

        subscription.unsubscribe().await.unwrap();

        sleep(Duration::from_secs(5)).await;

        session.destroy().await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }
}
