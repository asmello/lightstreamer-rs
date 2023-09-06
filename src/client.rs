use std::{fmt::Debug, str::Utf8Error, sync::Arc};

use tokio::net::TcpStream;
use tokio_tungstenite::{
    tungstenite::{self},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

use crate::{
    connection::{
        HttpConnection, HttpConnectionError, WebsocketConnection, WebsocketConnectionError,
    },
    session::Session,
    stream::DEFAULT_CAPACITY,
};

pub const LS_CID: &'static str = "mgQkwtwdysogQz2BJ4Ji kOj2Bg";
pub const LS_PROTOCOL_VERSION: &'static str = "TLCP-2.1.0";
pub const USER_AGENT: &'static str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);
pub const DEFAULT_ADAPTER_SET: &'static str = "DEFAULT";

#[derive(Debug)]
pub enum RequestError {
    Utf8Encoding(Utf8Error),
    UrlEncodingError(serde_urlencoded::ser::Error),
    ReqwestError(reqwest::Error),
}

#[derive(Debug)]
pub enum CreateSessionError {
    Http(HttpConnectionError),
    Websocket(WebsocketConnectionError),
}

#[derive(Debug)]
pub enum WebsocketError {
    BuildRequestError(http::Error),
    TungsteniteError(tokio_tungstenite::tungstenite::error::Error),
    UrlScheme,
}

#[derive(Debug)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

impl Credentials {
    pub fn new<S>(user: S, password: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            user: user.into(),
            password: password.into(),
        }
    }
}

#[derive(Debug)]
pub enum Transport {
    Http,
    Websocket,
}

pub struct ClientBuilder {
    adapter_set: String,
    base_url: Url,
    credentials: Option<Credentials>,
    user_agent: String,
    content_length: usize,
    broadcast_capacity: usize,
}

#[derive(Debug)]
pub enum ClientBuildError {
    ReqwestClientBuilder(reqwest::Error),
    InvalidUrlScheme(String),
}

impl ClientBuilder {
    pub fn adapter_set<S>(mut self, adapter_set: S) -> Self
    where
        S: Into<String>,
    {
        self.adapter_set = adapter_set.into();
        self
    }

    pub fn user_agent<S>(mut self, user_agent: S) -> Self
    where
        S: Into<String>,
    {
        self.user_agent = user_agent.into();
        self
    }

    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn content_length(mut self, content_length: usize) -> Self {
        self.content_length = content_length;
        self
    }

    pub fn broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity;
        self
    }

    pub fn build(self) -> Result<Client, ClientBuildError> {
        let http_client = reqwest::Client::builder()
            .user_agent(self.user_agent)
            .build()
            .map_err(ClientBuildError::ReqwestClientBuilder)?;

        let transport = match self.base_url.scheme() {
            "ws" | "wss" => Transport::Websocket,
            "http" | "https" => Transport::Http,
            other => return Err(ClientBuildError::InvalidUrlScheme(other.to_owned())),
        };

        Ok(Client {
            inner: Arc::new(ClientInner {
                adapter_set: self.adapter_set,
                base_url: self.base_url,
                credentials: self.credentials,
                content_length: self.content_length,
                broadcast_capacity: self.broadcast_capacity,
                transport,
                http_client,
            }),
        })
    }
}

#[derive(Debug)]
struct ClientInner {
    base_url: Url,
    http_client: reqwest::Client,
    adapter_set: String,
    content_length: usize,
    broadcast_capacity: usize,
    credentials: Option<Credentials>,
    transport: Transport,
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub fn builder(base_url: Url) -> ClientBuilder {
        ClientBuilder {
            adapter_set: DEFAULT_ADAPTER_SET.into(),
            user_agent: USER_AGENT.into(),
            base_url: base_url,
            credentials: None,
            content_length: usize::MAX,
            broadcast_capacity: DEFAULT_CAPACITY,
        }
    }

    pub fn credentials(&self) -> Option<&Credentials> {
        self.inner.credentials.as_ref()
    }

    pub fn base_url(&self) -> &Url {
        &self.inner.base_url
    }

    pub fn adapter_set(&self) -> &str {
        &self.inner.adapter_set
    }

    pub fn content_length(&self) -> usize {
        self.inner.content_length
    }

    pub fn broadcast_capacity(&self) -> usize {
        self.inner.broadcast_capacity
    }

    pub async fn create_session(&self) -> Result<Session, CreateSessionError> {
        match self.inner.transport {
            Transport::Http => HttpConnection::create(self.clone())
                .await
                .map(Session::new)
                .map_err(CreateSessionError::Http),
            Transport::Websocket => WebsocketConnection::create(self.clone())
                .await
                .map(Session::new)
                .map_err(CreateSessionError::Websocket),
        }
    }

    pub(crate) async fn create_websocket_connection(
        &self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, WebsocketError> {
        let mut url = self.base_url().clone();
        url.set_path("/lightstreamer");

        let request = http::Request::get(url.as_str())
            .header("Host", url.host().unwrap().to_string())
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header(
                "Sec-WebSocket-Key",
                tungstenite::handshake::client::generate_key(),
            )
            .header("Sec-WebSocket-Protocol", "TLCP-2.1.0.lightstreamer.com")
            .body(())
            .map_err(WebsocketError::BuildRequestError)?;

        tokio_tungstenite::connect_async(request)
            .await
            .map(|(stream, _)| stream)
            .map_err(WebsocketError::TungsteniteError)
    }

    pub(crate) async fn make_request(
        &self,
        base_url: &Url,
        request_name: &str,
        params: &[(&'static str, &str)],
    ) -> Result<reqwest::Response, RequestError> {
        let endpoint = base_url
            .join(&format!("/lightstreamer/{request_name}.txt"))
            .unwrap();

        let body = serde_urlencoded::to_string(params).map_err(RequestError::UrlEncodingError)?;

        let request = self
            .inner
            .http_client
            .post(endpoint)
            .query(&[("LS_protocol", LS_PROTOCOL_VERSION)])
            .body(body)
            .header("Content-Type", "text/plain");

        request
            .send()
            .await
            .map_err(RequestError::ReqwestError)?
            .error_for_status()
            .map_err(RequestError::ReqwestError)
    }
}
