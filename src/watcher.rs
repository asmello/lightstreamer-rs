use tokio::sync::{broadcast, watch};
use tracing::warn;

use crate::{notification::Notification, stream::NotificationStream};

pub struct NotificationWatcher {
    stream_rx: watch::Receiver<NotificationStream>,
    receiver: broadcast::Receiver<Notification>,
}

#[derive(Debug)]
pub enum WaitError {
    Closed,
}

impl NotificationWatcher {
    pub fn new(stream_rx: watch::Receiver<NotificationStream>) -> Self {
        let receiver = stream_rx.borrow().subscribe_notifications();
        Self {
            stream_rx,
            receiver,
        }
    }

    pub async fn wait_for<N>(&mut self) -> Result<N, WaitError>
    where
        N: TryFrom<Notification>,
    {
        loop {
            match self.receiver.recv().await {
                Ok(notification) => match <Notification as TryInto<N>>::try_into(notification) {
                    Ok(inner) => {
                        return Ok(inner);
                    }
                    _ => (),
                },
                Err(broadcast::error::RecvError::Closed) => {
                    if self.stream_rx.changed().await.is_err() {
                        warn!("streams channel closed (session presumed ended)");
                        return Err(WaitError::Closed);
                    }
                    // re-subscribe to notifications when new stream was created
                    self.receiver = self.stream_rx.borrow_and_update().subscribe_notifications();
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped = skipped, "notification watcher lagged");
                }
            }
        }
    }

    pub async fn wait_until<N>(&mut self, f: impl Fn(&N) -> bool) -> Result<N, WaitError>
    where
        N: TryFrom<Notification>,
    {
        loop {
            match self.receiver.recv().await {
                Ok(notification) => match <Notification as TryInto<N>>::try_into(notification) {
                    Ok(inner) => {
                        if f(&inner) {
                            return Ok(inner);
                        }
                    }
                    _ => (),
                },
                Err(broadcast::error::RecvError::Closed) => {
                    if self.stream_rx.changed().await.is_err() {
                        warn!("streams channel closed (session presumed ended)");
                        return Err(WaitError::Closed);
                    }
                    // re-subscribe to notifications when new stream was created
                    self.receiver = self.stream_rx.borrow_and_update().subscribe_notifications();
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped = skipped, "notification watcher lagged");
                }
            }
        }
    }
}
