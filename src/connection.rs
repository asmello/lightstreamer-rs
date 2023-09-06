use std::{fmt::Debug, num::ParseIntError, str::FromStr, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{broadcast, watch, Mutex},
    time,
};
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{
    client::{self, Credentials, RequestError, WebsocketError},
    notification::{
        parse_notification, ConnectionOkNotification, LoopNotification, Notification, ParsingError,
        RequestOkNotification,
    },
    stream::{Error, NotificationStream},
    url::UrlBuilder,
    watcher::NotificationWatcher,
    Client,
};

#[derive(Debug)]
pub enum ControlSendError {
    Request(RequestError),
    ReqwestError(reqwest::Error),
    MismatchedRequestId,
    UnexpectedResponse(Notification),
    ParsingError(ParsingError),
    ChannelClosed,
    EncodingError,
    SinkError,
}

#[derive(Debug)]
pub enum HttpConnectionError {
    RequestError(RequestError),
    ChannelClosed,
    Utf8Encoding,
    Url(crate::url::Error),
}

#[derive(Debug)]
pub enum WebsocketConnectionError {
    Client(WebsocketError),
    ChannelClosed,
    Utf8Encoding,
    UrlEncodingError(serde_urlencoded::ser::Error),
    SinkError,
}

#[derive(Debug)]
pub enum RecoverError {
    Http(HttpRebindError),
    Websocket(WebsocketReconnectError),
}

#[async_trait]
pub trait SessionConnection {
    fn session_id(&self) -> Arc<str>;

    fn stream(&self) -> watch::Receiver<NotificationStream>;

    async fn send(
        &self,
        operation: &'static str,
        op_params: &[(&'static str, &str)],
    ) -> Result<(), ControlSendError>;

    async fn recover(&self) -> Result<(), RecoverError>;
}

impl Debug for dyn SessionConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Connection[id={}]", self.session_id().as_ref())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub struct RequestId(u16);

impl From<u16> for RequestId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl FromStr for RequestId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse::<u16>()?))
    }
}

impl TryFrom<&[u8]> for RequestId {
    type Error = ParseIntError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(unsafe { std::str::from_utf8_unchecked(value) }
            .parse::<u16>()?
            .into())
    }
}

impl ToString for RequestId {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl RequestId {
    fn next(&self) -> RequestId {
        RequestId(self.0 + 1)
    }
}

struct HttpConnectionInner {
    client: Client,
    session_id: Arc<str>,
    control_url: Url,
    stream_tx: watch::Sender<NotificationStream>,
    next_request_id: Mutex<RequestId>,
}

#[derive(Clone)]
pub struct HttpConnection {
    inner: Arc<HttpConnectionInner>,
}

#[async_trait]
impl SessionConnection for HttpConnection {
    fn session_id(&self) -> Arc<str> {
        self.inner.session_id.clone()
    }

    fn stream(&self) -> watch::Receiver<NotificationStream> {
        self.inner.stream_tx.subscribe()
    }

    async fn send(
        &self,
        operation: &'static str,
        op_params: &[(&'static str, &str)],
    ) -> Result<(), ControlSendError> {
        let mut request_id = self.inner.next_request_id.lock().await;
        let request_id_str = request_id.to_string();

        debug!(id = ?request_id, operation = operation, params = ?op_params, "control request");

        let mut params = vec![
            ("LS_session", self.inner.session_id.as_ref()),
            ("LS_reqId", request_id_str.as_str()),
            ("LS_op", operation),
        ];
        params.extend(op_params.into_iter());

        let response = self
            .inner
            .client
            .make_request(&self.inner.control_url, "control", &params)
            .await
            .map_err(ControlSendError::Request)?;

        let mut body = response
            .bytes()
            .await
            .map_err(|err| ControlSendError::ReqwestError(err))?;

        body.truncate(body.len() - 2); // remove CRLF

        match parse_notification(body) {
            Ok(Notification::RequestOk(RequestOkNotification(request_id_in_response))) => {
                if *request_id != request_id_in_response {
                    return Err(ControlSendError::MismatchedRequestId);
                }
            }
            Ok(other) => return Err(ControlSendError::UnexpectedResponse(other)),
            Err(err) => return Err(ControlSendError::ParsingError(err)),
        }

        *request_id = request_id.next();

        Ok(())
    }

    async fn recover(&self) -> Result<(), RecoverError> {
        self.rebind().await.map_err(RecoverError::Http)
    }
}

#[derive(Debug)]
pub enum HttpRebindError {
    RequestError(RequestError),
    EncodingError,
    ChannelClosed,
    MismatchedSessionId { expected: Arc<str>, found: Arc<str> },
}

impl HttpConnection {
    pub async fn create(client: Client) -> Result<Self, HttpConnectionError> {
        let mut params = vec![("LS_cid", client::LS_CID)];

        if client.adapter_set() != client::DEFAULT_ADAPTER_SET {
            params.push(("LS_adapter_set", client.adapter_set()));
        }

        if let Some(Credentials { user, password }) = client.credentials() {
            params.extend_from_slice(&[("LS_user", user), ("LS_password", password)]);
        }

        let content_length = client.content_length();
        let content_length_str = content_length.to_string();
        if content_length != usize::MAX {
            params.push(("LS_content_length", content_length_str.as_ref()));
        }

        let response = client
            .make_request(client.base_url(), "create_session", &params)
            .await
            .map_err(HttpConnectionError::RequestError)?;

        let stream =
            NotificationStream::create_from_response(response, 0, client.broadcast_capacity());

        let (stream_tx, stream_rx) = watch::channel(stream);

        let notification = NotificationWatcher::new(stream_rx)
            .wait_for::<ConnectionOkNotification>()
            .await
            .map_err(|_| HttpConnectionError::ChannelClosed)?;

        let session_id = String::from_utf8(notification.session_id.into())
            .map_err(|_| HttpConnectionError::Utf8Encoding)?
            .into();

        let control_link = std::str::from_utf8(&notification.control_link)
            .map_err(|_| HttpConnectionError::Utf8Encoding)?;

        let control_url = if control_link == "*" {
            client.base_url().clone()
        } else {
            UrlBuilder::from(client.base_url())
                .host(control_link)
                .build()
                .map_err(HttpConnectionError::Url)?
        };

        let connection = Self {
            inner: Arc::new(HttpConnectionInner {
                client,
                session_id,
                control_url,
                stream_tx,
                next_request_id: Default::default(),
            }),
        };

        tokio::spawn(Self::process_notifications(connection.clone()));
        tokio::spawn(Self::process_errors(connection.clone()));

        Ok(connection)
    }

    async fn process_notifications(self) {
        let mut stream_rx = self.inner.stream_tx.subscribe();
        let mut notification_rx = stream_rx.borrow().subscribe_notifications();
        loop {
            match notification_rx.recv().await {
                Ok(notification) => {
                    match notification {
                        Notification::Loop(LoopNotification(delay)) => {
                            info!(delay = delay, "rebinding session");
                            time::sleep(Duration::from_millis(delay)).await;

                            match self.rebind().await {
                                Ok(()) => {
                                    notification_rx =
                                        stream_rx.borrow_and_update().subscribe_notifications();
                                }
                                Err(err) => {
                                    error!(err = ?err, "failed to rebind session");
                                    break;
                                }
                            }
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

    async fn rebind(&self) -> Result<(), HttpRebindError> {
        let mut next_request_id = self.inner.next_request_id.lock().await;

        let stream_rx = self.inner.stream_tx.subscribe();
        stream_rx.borrow().close();

        let data_count = stream_rx.borrow().data_count();
        let session_id = self.session_id();

        let mut watcher = NotificationWatcher::new(stream_rx);

        let recovery_from_str = data_count.to_string();
        let mut params = vec![
            ("LS_session", session_id.as_ref()),
            ("LS_recovery_from", recovery_from_str.as_str()),
        ];

        let content_length = self.inner.client.content_length();
        let content_length_str = content_length.to_string();
        if content_length != usize::MAX {
            params.push(("LS_content_length", content_length_str.as_str()));
        }

        let new_stream = self
            .inner
            .client
            .make_request(&self.inner.control_url, "bind_session", &params)
            .await
            .map(|response| {
                NotificationStream::create_from_response(
                    response,
                    data_count,
                    self.inner.client.broadcast_capacity(),
                )
            })
            .map_err(HttpRebindError::RequestError)?;

        self.inner.stream_tx.send_replace(new_stream);

        let connection_ok = watcher
            .wait_for::<ConnectionOkNotification>()
            .await
            .map_err(|_| HttpRebindError::ChannelClosed)?;

        let returned_session_id = String::from_utf8(connection_ok.session_id.into())
            .map_err(|_| HttpRebindError::EncodingError)?
            .into();

        if returned_session_id != session_id {
            return Err(HttpRebindError::MismatchedSessionId {
                expected: session_id,
                found: returned_session_id,
            });
        }

        // request id need only be unique within a connection - since we've reset the stream we can reset this too
        *next_request_id = RequestId::default();

        Ok(())
    }

    async fn process_errors(self) {
        let mut stream_rx = self.inner.stream_tx.subscribe();
        let mut error_rx = stream_rx.borrow().subscribe_errors();
        loop {
            match error_rx.recv().await {
                Ok(err) => {
                    match err {
                        Error::NotificationParsing(err) => {
                            warn!(err = ?err, "failed to parse remote notification");
                        }
                        Error::Reqwest(err) => {
                            error!(err = ?err, "network error, will attempt recovery");
                            match self.rebind().await {
                                Ok(()) => {
                                    error_rx = stream_rx.borrow_and_update().subscribe_errors();
                                }
                                Err(err) => {
                                    error!(err = ?err, "failed to recover session");
                                    break;
                                }
                            }
                        }
                        // this error only occurs in the websocket implementation
                        Error::Tungstenite(_) => unreachable!(),
                    };
                }
                Err(broadcast::error::RecvError::Closed) => {
                    if stream_rx.changed().await.is_err() {
                        error!("streams channel closed (session terminated abnormally)");
                        break;
                    }
                    // re-subscribe to errors when new stream was created
                    error_rx = stream_rx.borrow_and_update().subscribe_errors();
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped = skipped, "error processor lagging");
                }
            }
        }
    }
}

type WebsocketSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

struct WebsocketConnectionState {
    sink: WebsocketSink,
    next_request_id: RequestId,
}

struct WebsocketConnectionInner {
    client: Client,
    session_id: Arc<str>,
    stream_tx: watch::Sender<NotificationStream>,
    state: Mutex<WebsocketConnectionState>,
}

#[derive(Clone)]
pub struct WebsocketConnection {
    inner: Arc<WebsocketConnectionInner>,
}

#[async_trait]
impl SessionConnection for WebsocketConnection {
    fn session_id(&self) -> Arc<str> {
        self.inner.session_id.clone()
    }

    fn stream(&self) -> watch::Receiver<NotificationStream> {
        self.inner.stream_tx.subscribe()
    }

    async fn send(
        &self,
        operation: &'static str,
        op_params: &[(&'static str, &str)],
    ) -> Result<(), ControlSendError> {
        let mut state = self.inner.state.lock().await;

        let mut watcher = NotificationWatcher::new(self.inner.stream_tx.subscribe());

        let request_id = state.next_request_id;
        let request_id_str = request_id.to_string();

        debug!(id = ?request_id, operation = operation, params = ?op_params, "control request");

        let mut params = vec![
            ("LS_session", self.inner.session_id.as_ref()),
            ("LS_reqId", request_id_str.as_str()),
            ("LS_op", operation),
        ];
        params.extend(op_params.into_iter());

        let encoded_params =
            serde_urlencoded::to_string(params).map_err(|_| ControlSendError::EncodingError)?;

        let body = format!("control\r\n{encoded_params}\r\n");

        state
            .sink
            .send(Message::text(body))
            .await
            .map_err(|_| ControlSendError::SinkError)?;

        watcher
            .wait_until::<RequestOkNotification>(|notification| notification.0 == request_id)
            .await
            .map_err(|_| ControlSendError::ChannelClosed)?;

        state.next_request_id = request_id.next();

        Ok(())
    }

    async fn recover(&self) -> Result<(), RecoverError> {
        self.reconnect().await.map_err(RecoverError::Websocket)
    }
}

#[derive(Debug)]
pub enum WebsocketRebindError {
    SinkError,
    EncodingError,
    ChannelClosed,
}

#[derive(Debug)]
pub enum WebsocketReconnectError {
    Client(WebsocketError),
    Rebind(WebsocketRebindError),
}

impl WebsocketConnection {
    pub async fn create(client: Client) -> Result<Self, WebsocketConnectionError> {
        let ws_stream = client
            .create_websocket_connection()
            .await
            .map_err(WebsocketConnectionError::Client)?;

        let (mut sink, stream) = ws_stream.split();

        let notif_stream = NotificationStream::create_from_websocket_stream(
            stream,
            0,
            client.broadcast_capacity(),
        );

        let (stream_tx, stream_rx) = watch::channel(notif_stream);

        let mut watcher = NotificationWatcher::new(stream_rx.clone());

        let mut params = vec![("LS_cid", client::LS_CID)];

        if client.adapter_set() != client::DEFAULT_ADAPTER_SET {
            params.push(("LS_adapter_set", client.adapter_set()));
        }

        if let Some(Credentials { user, password }) = client.credentials() {
            params.extend_from_slice(&[("LS_user", user), ("LS_password", password)]);
        }

        let content_length = client.content_length();
        let content_length_str = content_length.to_string();
        if content_length != usize::MAX {
            params.push(("LS_content_length", content_length_str.as_ref()));
        }

        let encoded_params = serde_urlencoded::to_string(params)
            .map_err(WebsocketConnectionError::UrlEncodingError)?;

        let body = format!("create_session\r\n{encoded_params}\r\n");

        sink.send(Message::text(body))
            .await
            .map_err(|_| WebsocketConnectionError::SinkError)?;

        let ConnectionOkNotification { session_id, .. } = watcher
            .wait_for::<ConnectionOkNotification>()
            .await
            .map_err(|_| WebsocketConnectionError::ChannelClosed)?;

        let session_id = String::from_utf8(session_id.into())
            .map_err(|_| WebsocketConnectionError::Utf8Encoding)?
            .into();

        let connection = Self {
            inner: Arc::new(WebsocketConnectionInner {
                client,
                session_id,
                stream_tx,
                state: Mutex::new(WebsocketConnectionState {
                    sink,
                    next_request_id: Default::default(),
                }),
            }),
        };

        tokio::spawn(Self::process_notifications(connection.clone()));
        tokio::spawn(Self::process_errors(connection.clone()));

        Ok(connection)
    }

    async fn process_notifications(self) {
        let mut stream_rx = self.inner.stream_tx.subscribe();
        let mut notification_rx = stream_rx.borrow().subscribe_notifications();
        loop {
            match notification_rx.recv().await {
                Ok(notification) => {
                    match notification {
                        Notification::Loop(LoopNotification(delay)) => {
                            info!(delay = delay, "rebinding session");
                            time::sleep(Duration::from_millis(delay)).await;

                            let data_count = self.inner.stream_tx.borrow().data_count();

                            match self.rebind(data_count).await {
                                Ok(()) => {
                                    notification_rx =
                                        stream_rx.borrow_and_update().subscribe_notifications();
                                }
                                Err(err) => {
                                    error!(err = ?err, "failed to rebind session");
                                    break;
                                }
                            }
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

    async fn rebind(&self, data_count: u64) -> Result<(), WebsocketRebindError> {
        let mut state = self.inner.state.lock().await;

        let stream_rx = self.inner.stream_tx.subscribe();
        let session_id = self.session_id();

        let mut watcher = NotificationWatcher::new(stream_rx);

        debug!(data_count = data_count, "recovery from");

        let recovery_from_str = data_count.to_string();
        let mut params = vec![
            ("LS_session", session_id.as_ref()),
            ("LS_recovery_from", recovery_from_str.as_str()),
        ];

        let content_length = self.inner.client.content_length();
        let content_length_str = content_length.to_string();
        if content_length != usize::MAX {
            params.push(("LS_content_length", content_length_str.as_str()));
        }

        let encoded_params =
            serde_urlencoded::to_string(params).map_err(|_| WebsocketRebindError::EncodingError)?;

        let body = format!("bind_session\r\n{encoded_params}\r\n");

        state
            .sink
            .send(Message::text(body))
            .await
            .map_err(|_| WebsocketRebindError::SinkError)?;

        watcher
            .wait_until::<ConnectionOkNotification>(|notification| {
                // todo: error handling on UTF8 error
                std::str::from_utf8(&notification.session_id).unwrap() == session_id.as_ref()
            })
            .await
            .map_err(|_| WebsocketRebindError::ChannelClosed)?;

        Ok(())
    }

    async fn process_errors(self) {
        let mut stream_rx = self.inner.stream_tx.subscribe();
        let mut error_rx = stream_rx.borrow().subscribe_errors();
        loop {
            match error_rx.recv().await {
                Ok(error) => {
                    match error {
                        Error::NotificationParsing(err) => {
                            warn!(err = ?err, "failed to parse remote notification");
                        }
                        Error::Tungstenite(err) => {
                            error!(err = ?err, "network error, will attempt recovery");
                            match self.reconnect().await {
                                Ok(()) => {
                                    error_rx = stream_rx.borrow_and_update().subscribe_errors();
                                }
                                Err(err) => {
                                    error!(err = ?err, "failed to recover session");
                                    break;
                                }
                            }
                        }
                        // this error only occurs in the HTTP implementation
                        Error::Reqwest(_) => unreachable!(),
                    };
                }
                Err(broadcast::error::RecvError::Closed) => {
                    if stream_rx.changed().await.is_err() {
                        error!("streams channel closed (session terminated abnormally)");
                        break;
                    }
                    // re-subscribe to notifications when new stream was created
                    error_rx = stream_rx.borrow_and_update().subscribe_errors();
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped = skipped, "error processor lagging");
                }
            }
        }
    }

    async fn reconnect(&self) -> Result<(), WebsocketReconnectError> {
        let data_count = self.inner.stream_tx.borrow().data_count();

        {
            let mut state = self.inner.state.lock().await;

            let ws_stream = self
                .inner
                .client
                .create_websocket_connection()
                .await
                .map_err(WebsocketReconnectError::Client)?;

            let (sink, stream) = ws_stream.split();

            let notif_stream = NotificationStream::create_from_websocket_stream(
                stream,
                data_count,
                self.inner.client.broadcast_capacity(),
            );

            self.inner.stream_tx.send_replace(notif_stream);

            state.sink = sink;
            state.next_request_id = RequestId::default();
        }

        self.rebind(data_count)
            .await
            .map_err(WebsocketReconnectError::Rebind)
    }
}
