use std::fmt::Debug;
use std::{borrow::Borrow, str::FromStr, sync::Arc};

use bytes::Bytes;
use tokio::sync::{
    broadcast::{self},
    mpsc,
    watch::{self, Ref},
};
use tracing::{error, warn};

use crate::session::TransportError;
use crate::{
    notification::{Notification, UnsubscriptionNotification, UpdateNotification},
    session::Session,
    stream::NotificationStream,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

impl Default for SubscriptionId {
    fn default() -> Self {
        Self(1) // spec says it should start at 1 even though 0 works too
    }
}

#[derive(Debug)]
pub struct ParseSubscriptionIdError;

impl FromStr for SubscriptionId {
    type Err = ParseSubscriptionIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>()
            .map(|num| SubscriptionId(num))
            .map_err(|_| ParseSubscriptionIdError)
    }
}

impl ToString for SubscriptionId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl SubscriptionId {
    pub fn next(&self) -> SubscriptionId {
        SubscriptionId(self.0 + 1)
    }
}

#[derive(Debug)]
pub struct ItemUpdate {
    index: usize,
    field_updates: Arc<[ValueUpdate]>,
}

impl ItemUpdate {
    pub fn item_index(&self) -> usize {
        self.index
    }

    pub fn updates(&self) -> &[ValueUpdate] {
        &self.field_updates
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueUpdate {
    Unchanged,
    Changed(FieldValue),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldValue {
    Null,
    Empty,
    Value(Bytes),
}

#[derive(Clone, Debug)]
pub struct Item {
    index: usize,
    fields_rx: watch::Receiver<Box<[FieldValue]>>,
}

impl Item {
    pub(crate) fn new(
        mut stream_rx: watch::Receiver<NotificationStream>,
        mut notification_rx: broadcast::Receiver<Notification>,
        parent_id: SubscriptionId,
        index: usize,
        num_fields: usize,
        snapshot_tx: broadcast::Sender<ItemSnapshot>,
    ) -> Self {
        let fields: Box<[FieldValue]> = std::iter::repeat(FieldValue::Null)
            .take(num_fields)
            .collect();

        let (fields_tx, fields_rx) = watch::channel(fields);

        tokio::spawn(async move {
            let fields_rx = fields_tx.subscribe();
            loop {
                match notification_rx.recv().await {
                    Ok(notification) => match notification {
                        Notification::Update(UpdateNotification {
                            subscription_id,
                            item_index,
                            updates,
                        }) => {
                            if subscription_id != parent_id || item_index != index {
                                continue;
                            }
                            fields_tx.send_modify(|fields| {
                                for (i, update) in updates.into_iter().enumerate() {
                                    if let ValueUpdate::Changed(new_value) = update {
                                        fields[i] = new_value.clone();
                                    }
                                }
                            });
                            // only broadcast a snapshot if there is at least one subscriber
                            if snapshot_tx.receiver_count() > 0 {
                                if snapshot_tx
                                    .send(ItemSnapshot {
                                        fields: fields_rx.borrow().into_iter().cloned().collect(),
                                    })
                                    .is_err()
                                {
                                    error!(
                                        subscription_id = ?parent_id,
                                        item_index = index,
                                        "failed to send item snapshot"
                                    );
                                }
                            }
                        }
                        Notification::Unsubscription(UnsubscriptionNotification(
                            subscription_id,
                        )) => {
                            if subscription_id == parent_id {
                                break;
                            }
                        }
                        Notification::SessionEnded(_) => {
                            break;
                        }
                        _ => (),
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        if stream_rx.changed().await.is_err() {
                            warn!("streams channel closed (session presumed ended)");
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
        });

        Self { index, fields_rx }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn fields(&self) -> Ref<'_, Box<[FieldValue]>> {
        self.fields_rx.borrow()
    }

    pub fn snapshot(&self) -> ItemSnapshot {
        self.into()
    }
}

#[derive(Clone, Debug)]
pub struct ItemSnapshot {
    fields: Arc<[FieldValue]>,
}

impl ItemSnapshot {
    pub fn fields(&self) -> &[FieldValue] {
        &self.fields
    }
}

impl<I> From<I> for ItemSnapshot
where
    I: Borrow<Item>,
{
    fn from(value: I) -> Self {
        Self {
            fields: value
                .borrow()
                .fields_rx
                .borrow()
                .into_iter()
                .cloned()
                .collect(),
        }
    }
}

#[derive(Debug)]
struct SubscriptionConfiguration {
    max_frequency: f64,
    filtered: bool,
}

impl Default for SubscriptionConfiguration {
    fn default() -> Self {
        Self {
            max_frequency: f64::INFINITY,
            filtered: false,
        }
    }
}

#[derive(Debug)]
pub struct ChannelClosed;

pub struct Subscription<'a> {
    session: &'a Session,

    id: SubscriptionId,
    items: Box<[Item]>,
    num_fields: usize,

    configuration_rx: watch::Receiver<SubscriptionConfiguration>,
    active_rx: watch::Receiver<bool>,

    // a sender only because we don't want an active snapshots subscription running all the time
    snapshot_tx: broadcast::Sender<ItemSnapshot>,
}

impl Subscription<'_> {
    pub(crate) fn new<'a>(
        session: &'a Session,
        id: SubscriptionId,
        num_items: usize,
        num_fields: usize,
    ) -> Subscription<'a> {
        let stream_rx = session.stream();
        let notification_rx = stream_rx.borrow().subscribe_notifications();
        let (configuration_tx, configuration_rx) =
            watch::channel(SubscriptionConfiguration::default());
        let (active_tx, active_rx) = watch::channel(true);
        let (snapshot_tx, snapshot_rx) = broadcast::channel(128);
        std::mem::drop(snapshot_rx); // disable snapshot broadcasting by default, as it's relatively expensive

        let items: Box<[Item]> = (1..=num_items)
            .map(|i| {
                Item::new(
                    stream_rx.clone(),
                    notification_rx.resubscribe(),
                    id,
                    i,
                    num_fields,
                    snapshot_tx.clone(),
                )
            })
            .collect();

        tokio::spawn(process_notifications(
            id,
            stream_rx,
            notification_rx,
            configuration_tx,
            active_tx,
        ));

        Subscription {
            id,
            num_fields,
            items,
            session,
            configuration_rx,
            active_rx,
            snapshot_tx,
        }
    }

    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    pub fn num_items(&self) -> usize {
        self.items.len()
    }

    pub fn num_fields(&self) -> usize {
        self.num_fields
    }

    pub fn active(&self) -> watch::Receiver<bool> {
        self.active_rx.clone()
    }

    pub fn get_max_frequency(&self) -> f64 {
        self.configuration_rx.borrow().max_frequency
    }

    pub fn get_filtered(&self) -> bool {
        self.configuration_rx.borrow().filtered
    }

    pub async fn unsubscribe(&self) -> Result<(), TransportError> {
        self.session.unsubscribe(self.id).await
    }

    pub fn updates(&self) -> mpsc::Receiver<ItemUpdate> {
        let mut stream_rx = self.session.stream();
        let (update_tx, update_rx) = mpsc::channel(stream_rx.borrow().capacity());
        let mut notification_rx = stream_rx.borrow().subscribe_notifications();
        let subscription_id = self.id;

        tokio::spawn(async move {
            loop {
                match notification_rx.recv().await {
                    Ok(notification) => match notification {
                        Notification::Update(notification) => {
                            if notification.subscription_id == subscription_id {
                                let update = ItemUpdate {
                                    index: notification.item_index,
                                    field_updates: notification.updates,
                                };
                                if update_tx.send(update).await.is_err() {
                                    break;
                                };
                            }
                        }
                        Notification::Unsubscription(UnsubscriptionNotification(
                            unsubscribed_id,
                        )) => {
                            if unsubscribed_id == subscription_id {
                                break;
                            }
                        }
                        Notification::SessionEnded(_) => {
                            break;
                        }
                        _ => (),
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        if stream_rx.changed().await.is_err() {
                            warn!("streams channel closed (session presumed ended)");
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
        });

        update_rx
    }

    // 1-based index to be consistent with protocol
    pub fn item(&self, index: usize) -> &Item {
        &self.items[index - 1]
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn snapshot(&self) -> Vec<ItemSnapshot> {
        self.items.iter().map(ItemSnapshot::from).collect()
    }

    pub fn snapshots(&self) -> broadcast::Receiver<ItemSnapshot> {
        self.snapshot_tx.subscribe()
    }

    pub async fn changed(&mut self) -> Result<ItemSnapshot, ChannelClosed> {
        let mut subscription = self.snapshot_tx.subscribe();
        loop {
            match subscription.recv().await {
                Ok(snapshot) => return Ok(snapshot),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    warn!(subscription_id = ?self.id, "lagged while waiting for a change");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(ChannelClosed);
                }
            }
        }
    }
}

async fn process_notifications(
    id: SubscriptionId,

    mut stream_rx: watch::Receiver<NotificationStream>,
    mut notification_rx: broadcast::Receiver<Notification>,

    configuration_tx: watch::Sender<SubscriptionConfiguration>,
    active_tx: watch::Sender<bool>,
) {
    loop {
        match notification_rx.recv().await {
            Ok(notification) => match notification {
                Notification::SubscriptionConfiguration(notification) => {
                    if notification.subscription_id == id {
                        configuration_tx.send_if_modified(|value| {
                            let mut modified = false;
                            if value.filtered != notification.filtered {
                                value.filtered = notification.filtered;
                                modified = true;
                            }
                            if value.max_frequency != notification.max_frequency {
                                value.max_frequency = notification.max_frequency;
                                modified = true;
                            }
                            modified
                        });
                    }
                }
                Notification::Unsubscription(UnsubscriptionNotification(subscription_id)) => {
                    if subscription_id == id {
                        active_tx.send_replace(false);
                        break;
                    }
                }
                Notification::SessionEnded(_) => {
                    active_tx.send_replace(false);
                    break;
                }
                _ => (),
            },
            Err(broadcast::error::RecvError::Closed) => {
                if stream_rx.changed().await.is_err() {
                    warn!("streams channel closed (session presumed ended)");
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
