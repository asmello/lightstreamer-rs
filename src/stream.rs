use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{debug, error, info, warn};

use crate::notification::{
    parse_notification, Notification, ParsingError, ProgressiveNotification,
};

pub const DEFAULT_CAPACITY: usize = 128;

#[derive(Clone, Debug)]
pub enum Error {
    Reqwest(Arc<reqwest::Error>),
    Tungstenite(Arc<tungstenite::Error>),
    NotificationParsing(Arc<ParsingError>),
}

#[derive(Debug)]
struct NotificationStreamInner {
    capacity: usize,
    data_count_rx: watch::Receiver<u64>,
    notification_rx: broadcast::Receiver<Notification>,
    error_rx: broadcast::Receiver<Error>,
    closed_tx: watch::Sender<bool>,
}

#[derive(Clone, Debug)]
pub struct NotificationStream {
    inner: Arc<NotificationStreamInner>,
}

impl NotificationStream {
    pub(crate) fn create_from_response(
        response: reqwest::Response,
        previous_data_count: u64,
        capacity: usize,
    ) -> Self {
        let (notification_tx, notification_rx) = broadcast::channel(capacity);
        let (error_tx, error_rx) = broadcast::channel(capacity);
        let (data_count_tx, data_count_rx) = watch::channel(previous_data_count);
        let (closed_tx, closed_rx) = watch::channel(false);

        tokio::spawn(process_response_stream(
            response,
            notification_tx,
            error_tx,
            data_count_tx,
            closed_rx,
        ));

        Self {
            inner: Arc::new(NotificationStreamInner {
                capacity,
                notification_rx,
                error_rx,
                data_count_rx,
                closed_tx,
            }),
        }
    }

    pub(crate) fn create_from_websocket_stream<S>(
        stream: S,
        previous_data_count: u64,
        capacity: usize,
    ) -> Self
    where
        S: Stream<Item = Result<Message, tungstenite::error::Error>> + Send + Sync + 'static,
    {
        let (notification_tx, notification_rx) = broadcast::channel(capacity);
        let (error_tx, error_rx) = broadcast::channel(capacity);
        let (data_count_tx, data_count_rx) = watch::channel(previous_data_count);
        let (closed_tx, closed_rx) = watch::channel(false);

        tokio::spawn(process_websocket_stream(
            stream,
            notification_tx,
            error_tx,
            data_count_tx,
            closed_rx,
        ));

        Self {
            inner: Arc::new(NotificationStreamInner {
                capacity,
                notification_rx,
                error_rx,
                data_count_rx,
                closed_tx,
            }),
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn close(&self) {
        self.inner.closed_tx.send_replace(true);
    }

    pub fn is_closed(&self) -> watch::Ref<'_, bool> {
        self.inner.closed_tx.borrow()
    }

    pub fn subscribe_notifications(&self) -> broadcast::Receiver<Notification> {
        self.inner.notification_rx.resubscribe()
    }

    pub fn subscribe_errors(&self) -> broadcast::Receiver<Error> {
        self.inner.error_rx.resubscribe()
    }

    pub fn data_count(&self) -> u64 {
        *self.inner.data_count_rx.borrow()
    }
}

async fn process_response_stream(
    mut response: reqwest::Response,

    notification_tx: broadcast::Sender<Notification>,
    error_tx: broadcast::Sender<Error>,
    data_count_tx: watch::Sender<u64>,

    closed_rx: watch::Receiver<bool>,
) {
    const INITIAL_SIZE: usize = 128;
    let data_count_rx = data_count_tx.subscribe();
    let mut buffer = Vec::with_capacity(INITIAL_SIZE);
    let mut skip = 0;

    while !*closed_rx.borrow() {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                for byte in chunk {
                    let n = buffer.len();
                    if byte == b'\n' && n > 0 && buffer[n - 1] == b'\r' {
                        buffer.truncate(n - 1);
                        let line = Bytes::from(buffer);
                        buffer = Vec::with_capacity(INITIAL_SIZE);
                        match parse_notification(line) {
                            Ok(notification) => {
                                debug!(notification = ?notification);

                                // data notifications can be resumed, but we must track their count
                                if notification.is_data() {
                                    if skip > 0 {
                                        debug!(
                                            skip = skip,
                                            "not broadcasting repeated data notification"
                                        );
                                        skip -= 1;
                                        continue;
                                    }
                                    // caught up to previous data count by now, start counting again
                                    data_count_tx.send_modify(|count| *count += 1);
                                }

                                if let Notification::Progressive(ProgressiveNotification(
                                    progressive,
                                )) = notification
                                {
                                    info!(progressive = progressive, "resuming from");
                                    let previous_data_count = *data_count_rx.borrow();
                                    if progressive < previous_data_count {
                                        // stream is resuming from before previous point, so we must skip
                                        // some data notifications to avoid duplication
                                        skip = previous_data_count - progressive;
                                    }
                                    debug!("note: PROG notification is not broadcast");
                                    continue;
                                }

                                while let Err(err) = notification_tx.send(notification) {
                                    error!(err = ?err, "orphaned stream, aborting");
                                    return;
                                }
                            }
                            Err(err) => {
                                debug!(err = ?err, "failed to parse");

                                if let Err(err) =
                                    error_tx.send(Error::NotificationParsing(Arc::new(err)))
                                {
                                    error!(err = ?err.0, "no error handlers registered");
                                }
                            }
                        }
                    } else {
                        buffer.push(byte);
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                warn!(err = ?err, "terminating stream");
                if error_tx.send(Error::Reqwest(Arc::new(err))).is_err() {
                    error!("no error handlers registered");
                };
                break;
            }
        }
    }
}

async fn process_websocket_stream<S>(
    stream: S,

    notification_tx: broadcast::Sender<Notification>,
    error_tx: broadcast::Sender<Error>,
    data_count_tx: watch::Sender<u64>,

    closed_rx: watch::Receiver<bool>,
) where
    S: Stream<Item = Result<Message, tungstenite::error::Error>>,
{
    const INITIAL_SIZE: usize = 128;
    let data_count_rx = data_count_tx.subscribe();
    let mut buffer = Vec::with_capacity(INITIAL_SIZE);
    let mut skip = 0;

    tokio::pin!(stream);

    while !*closed_rx.borrow() {
        if let Some(result) = stream.next().await {
            match result {
                Ok(msg) => {
                    for byte in msg.into_data() {
                        let n = buffer.len();
                        if byte == b'\n' && n > 0 && buffer[n - 1] == b'\r' {
                            buffer.truncate(n - 1);
                            let line = Bytes::from(buffer);
                            buffer = Vec::with_capacity(INITIAL_SIZE);
                            match parse_notification(line) {
                                Ok(notification) => {
                                    debug!(notification = ?notification);

                                    // data notifications can be resumed, but we must track their count
                                    if notification.is_data() {
                                        if skip > 0 {
                                            debug!(
                                                skip = skip,
                                                "not broadcasting repeated data notification"
                                            );
                                            skip -= 1;
                                            continue;
                                        }
                                        // caught up to previous data count by now, start counting again
                                        data_count_tx.send_modify(|count| *count += 1);
                                    }

                                    if let Notification::Progressive(ProgressiveNotification(
                                        progressive,
                                    )) = notification
                                    {
                                        info!(progressive = progressive, "resuming from");
                                        let previous_data_count = *data_count_rx.borrow();
                                        if progressive < previous_data_count {
                                            // stream is resuming from before previous point, so we must skip
                                            // some data notifications to avoid duplication
                                            skip = previous_data_count - progressive;
                                        }
                                        debug!("note: PROG notification is not broadcast");
                                        continue;
                                    }

                                    while let Err(err) = notification_tx.send(notification) {
                                        error!(err = ?err, "orphaned stream, aborting");
                                        return;
                                    }
                                }
                                Err(err) => {
                                    debug!(err = ?err, "failed to parse");

                                    if let Err(err) =
                                        error_tx.send(Error::NotificationParsing(Arc::new(err)))
                                    {
                                        error!(err = ?err.0, "no error handlers registered");
                                    }
                                }
                            }
                        } else {
                            buffer.push(byte);
                        }
                    }
                }
                Err(err) => {
                    warn!(err = ?err, "terminating stream");
                    if error_tx.send(Error::Tungstenite(Arc::new(err))).is_err() {
                        error!("no error handlers registered");
                    };
                    break;
                }
            }
        } else {
            break;
        }
    }
}
