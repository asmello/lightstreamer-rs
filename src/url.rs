use url::Url;

#[derive(Default)]
pub struct UrlBuilder {
    scheme: String,
    host: Option<String>,
    path: Option<String>,
}

#[derive(Debug)]
pub enum Error {
    MissingHost,
    ParseError(url::ParseError),
}

impl UrlBuilder {
    // pub fn new() -> Self {
    //     Self {
    //         scheme: "https".into(),
    //         ..Default::default()
    //     }
    // }

    // pub fn scheme<S>(mut self, scheme: S) -> Self
    // where
    //     S: Into<String>,
    // {
    //     self.scheme = scheme.into();
    //     self
    // }

    pub fn host<S>(mut self, host: S) -> Self
    where
        S: Into<String>,
    {
        self.host = Some(host.into());
        self
    }

    // pub fn path<S>(mut self, path: S) -> Self
    // where
    //     S: Into<String>,
    // {
    //     self.path = Some(path.into());
    //     self
    // }

    pub fn build(self) -> Result<Url, Error> {
        let mut url_str = self.scheme;
        url_str.push_str("://");
        url_str.push_str(&self.host.ok_or(Error::MissingHost)?);

        if let Some(path) = self.path {
            url_str.push_str(&format!("/{path}"));
        }

        Url::parse(&url_str).map_err(Error::ParseError)
    }
}

impl From<Url> for UrlBuilder {
    fn from(url: Url) -> Self {
        Self {
            scheme: url.scheme().into(),
            host: url.host_str().map(String::from),
            path: Some(url.path().into()),
        }
    }
}

impl From<&Url> for UrlBuilder {
    fn from(url: &Url) -> Self {
        Self {
            scheme: url.scheme().into(),
            host: url.host_str().map(String::from),
            path: Some(url.path().into()),
        }
    }
}
