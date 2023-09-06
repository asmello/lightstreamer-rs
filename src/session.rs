use std::fmt::Debug;
use std::net::IpAddr;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{broadcast, watch, Mutex};
use tracing::{error, info, warn};

use crate::connection::{ControlSendError, RecoverError, SessionConnection};
use crate::notification::{
    ClientIpNotification, Notification, ServerNameNotification, SessionEndedNotification,
    SubscriptionNotification, UnsubscriptionNotification,
};
use crate::stream::NotificationStream;
use crate::subscription::{Subscription, SubscriptionId};
use crate::watcher::NotificationWatcher;

#[derive(Debug)]
pub enum TransportError {
    ControlSendError(ControlSendError),
    ChannelClosed,
}

#[derive(Debug)]
pub enum SubscriptionMode {
    // Raw,
    Merge,
    // Distinct,
    // Command,
}

impl SubscriptionMode {
    fn to_str(&self) -> &'static str {
        match self {
            // SubscriptionMode::Raw => "RAW",
            SubscriptionMode::Merge => "MERGE",
            // SubscriptionMode::Distinct => "DISTINCT",
            // SubscriptionMode::Command => "COMMAND",
        }
    }
}

#[derive(Debug)]
pub struct Session {
    controller: Box<dyn SessionConnection>,
    server_name_rx: watch::Receiver<Option<Bytes>>,
    client_ip_rx: watch::Receiver<Option<IpAddr>>,
    ended_rx: watch::Receiver<bool>,
    next_subscription_id: Arc<Mutex<SubscriptionId>>,
}

impl Session {
    pub fn new<C>(controller: C) -> Self
    where
        C: SessionConnection + Clone + Send + Sync + 'static,
    {
        let (client_ip_tx, client_ip_rx) = watch::channel(None);
        let (server_name_tx, server_name_rx) = watch::channel(None);
        let (ended_tx, ended_rx) = watch::channel(false);

        tokio::spawn(process_notifications(
            controller.clone(),
            client_ip_tx,
            server_name_tx,
            ended_tx,
        ));

        Self {
            controller: Box::new(controller),
            client_ip_rx,
            server_name_rx,
            ended_rx,
            next_subscription_id: Default::default(),
        }
    }

    pub fn id(&self) -> Arc<str> {
        self.controller.session_id()
    }

    pub fn stream(&self) -> watch::Receiver<NotificationStream> {
        self.controller.stream()
    }

    pub fn ended(&self) -> watch::Receiver<bool> {
        self.ended_rx.clone()
    }

    pub fn get_client_ip(&self) -> Option<IpAddr> {
        self.client_ip_rx.borrow().clone()
    }

    pub fn get_server_name(&self) -> Option<Bytes> {
        self.server_name_rx.borrow().clone()
    }

    pub async fn subscribe(
        &self,
        data_adapter: &str,
        items: &[&str],
        fields: &[&str],
        mode: SubscriptionMode,
    ) -> Result<Subscription, TransportError> {
        let mut next_subscription_id = self.next_subscription_id.lock().await;
        let subscription_id_str = next_subscription_id.to_string();
        let joined_items = items.join(" ");
        let joined_fields = fields.join(" ");

        let op_params = &[
            ("LS_data_adapter", data_adapter),
            ("LS_group", joined_items.as_str()),
            ("LS_schema", joined_fields.as_str()),
            ("LS_subId", subscription_id_str.as_str()),
            ("LS_mode", mode.to_str()),
        ];

        let mut watcher = NotificationWatcher::new(self.controller.stream());

        self.controller
            .send("add", op_params)
            .await
            .map_err(TransportError::ControlSendError)?;

        let SubscriptionNotification {
            subscription_id,
            num_items,
            num_fields,
        } = watcher
            .wait_until::<SubscriptionNotification>(|notification| {
                notification.subscription_id == *next_subscription_id
            })
            .await
            .map_err(|_| TransportError::ChannelClosed)?;

        info!(id = ?subscription_id, "created");

        let subscription = Subscription::new(self, subscription_id, num_items, num_fields);

        *next_subscription_id = subscription_id.next();

        Ok(subscription)
    }

    pub async fn unsubscribe(&self, subscription_id: SubscriptionId) -> Result<(), TransportError> {
        let subscription_id_str = subscription_id.to_string();

        let mut watcher = NotificationWatcher::new(self.controller.stream());

        self.controller
            .send("delete", &[("LS_subId", subscription_id_str.as_str())])
            .await
            .map_err(TransportError::ControlSendError)?;

        watcher
            .wait_until::<UnsubscriptionNotification>(|notification| {
                notification.0 == subscription_id
            })
            .await
            .map_err(|_| TransportError::ChannelClosed)?;

        Ok(())
    }

    pub async fn recover(&self) -> Result<(), RecoverError> {
        self.controller.recover().await
    }

    pub async fn destroy(&self) -> Result<(), TransportError> {
        let mut watcher = NotificationWatcher::new(self.controller.stream());

        self.controller
            .send("destroy", &[])
            .await
            .map_err(TransportError::ControlSendError)?;

        if watcher
            .wait_for::<SessionEndedNotification>()
            .await
            .is_err()
        {
            return Err(TransportError::ChannelClosed);
        }

        Ok(())
    }
}

async fn process_notifications<C>(
    controller: C,

    client_ip_tx: watch::Sender<Option<IpAddr>>,
    server_name_tx: watch::Sender<Option<Bytes>>,
    ended_tx: watch::Sender<bool>,
) where
    C: SessionConnection,
{
    let mut stream_rx = controller.stream();
    let mut notification_rx = stream_rx.borrow().subscribe_notifications();
    loop {
        match notification_rx.recv().await {
            Ok(notification) => {
                match notification {
                    Notification::ClientIp(ClientIpNotification(ip)) => {
                        client_ip_tx.send_modify(|value| {
                            value.replace(ip);
                        });
                    }
                    Notification::ServerName(ServerNameNotification(name)) => {
                        server_name_tx.send_modify(|value| {
                            value.replace(name);
                        });
                    }
                    Notification::SessionEnded(notification) => {
                        info!(
                            cause = notification.cause_code,
                            message = std::str::from_utf8(&notification.cause_message).ok(),
                            "session ended"
                        );
                        ended_tx.send_replace(true);
                        break;
                    }
                    _ => (),
                };
            }
            Err(broadcast::error::RecvError::Closed) => {
                if stream_rx.changed().await.is_err() {
                    error!("streams channel closed (session terminated abnormally)");
                    break;
                }
                // re-subscribe to notifications when new stream was created
                notification_rx = stream_rx.borrow_and_update().subscribe_notifications();
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                warn!(skipped = skipped, "notification processor lagging");
            }
        }
    }
}
