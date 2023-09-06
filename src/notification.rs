use std::{
    net::{AddrParseError, IpAddr},
    num::{ParseFloatError, ParseIntError},
    str::{FromStr, Utf8Error},
    sync::Arc,
};

use bytes::Bytes;

use crate::{
    connection::RequestId,
    subscription::{FieldValue, ParseSubscriptionIdError, SubscriptionId, ValueUpdate},
    util::Either,
};

#[derive(Clone, Debug)]
pub enum Notification {
    ConnectionOk(ConnectionOkNotification),
    SessionFailed(SessionCreationFailedNotification),
    SessionEnded(SessionEndedNotification),
    ServerName(ServerNameNotification),
    ClientIp(ClientIpNotification),
    Noop(NoopNotification),
    Constrain(ConstrainNotification),
    Probe(ProbeNotification),
    Loop(LoopNotification),
    Subscription(SubscriptionNotification),
    // SubscriptionCommand(SubscriptionCommandNotification),
    Unsubscription(UnsubscriptionNotification),
    // EndOfSnapshot(EndOfSnapshotNotification),
    // ClearSnapshot(ClearSnapshotNotification),
    // Overflow(OverflowNotification),
    SubscriptionConfiguration(SubscriptionConfigurationNotification),
    Sync(SyncNotification),
    Update(UpdateNotification),
    Progressive(ProgressiveNotification),

    RequestOk(RequestOkNotification),
    RequestError(RequestErrorNotification),
    Error(ErrorNotification),
}

#[derive(Clone, Debug)]
pub struct RequestOkNotification(pub RequestId);

impl TryFrom<Notification> for RequestOkNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::RequestOk(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestErrorNotification {
    pub request_id: usize,
    pub error_code: usize,
    pub error_message: Bytes,
}

impl RequestErrorNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        mut parts: impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        let request_id = parse_next("request_id", &mut parts)?;
        let error_code = parse_next("error_code", &mut parts)?;
        let error_message = next_bytes("error_message", buffer, &mut parts)?;
        Ok(Self {
            request_id,
            error_code,
            error_message,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ErrorNotification {
    pub error_code: usize,
    pub error_message: Bytes,
}

impl ErrorNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        mut parts: impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        let error_code = parse_next("error_code", &mut parts)?;
        let error_message = next_bytes("error_message", buffer, &mut parts)?;
        Ok(Self {
            error_code,
            error_message,
        })
    }
}

impl Notification {
    pub fn is_data(&self) -> bool {
        match self {
            Self::Update(_)
            | Self::Subscription(_)
            | Self::Unsubscription(_)
            | Self::SubscriptionConfiguration(_) => true,
            _ => false,
        }
    }
}

fn parse_next<'a, T>(
    name: &'static str,
    parts: &mut impl Iterator<Item = &'a [u8]>,
) -> Result<T, ParsingErrorCause>
where
    T: FromStr,
    ParsingErrorCause: From<T::Err>,
{
    let part = parts
        .next()
        .ok_or(ParsingErrorCause::MissingArgument(name))?;
    // we don't need to validate encoding because if it's wrong, we'll fail on parsing next
    let s = unsafe { std::str::from_utf8_unchecked(part) };
    s.parse::<T>().map_err(ParsingErrorCause::from)
}

fn next_bytes<'a>(
    name: &'static str,
    buffer: &Bytes,
    parts: &mut impl Iterator<Item = &'a [u8]>,
) -> Result<Bytes, ParsingErrorCause> {
    parts
        .next()
        .map(|slice| buffer.slice_ref(slice))
        .ok_or(ParsingErrorCause::MissingArgument(name))
}

unsafe fn next_str_unchecked<'a>(
    name: &'static str,
    parts: &mut impl Iterator<Item = &'a [u8]>,
) -> Result<&'a str, ParsingErrorCause> {
    let bytes = parts
        .next()
        .ok_or(ParsingErrorCause::MissingArgument(name))?;
    Ok(std::str::from_utf8_unchecked(bytes))
}

#[derive(Copy, Clone, Debug)]
pub struct ClientIpNotification(pub IpAddr);

#[derive(Clone, Debug)]
pub struct NoopNotification(pub Bytes);

#[derive(Copy, Clone, Debug)]
pub struct ConstrainNotification(pub usize);

impl ConstrainNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        let bytes = next_bytes("bandwidth", buffer, parts)?;
        // we don't need to validate encoding because if it's wrong, we'll fail on parsing next
        let bandwidth = match unsafe { std::str::from_utf8_unchecked(&bytes) } {
            "unlimited" => usize::MAX,
            other => other
                .parse::<usize>()
                .map_err(ParsingErrorCause::InvalidInteger)?,
        };

        Ok(Self(bandwidth))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ProbeNotification;

#[derive(Copy, Clone, Debug)]
pub struct LoopNotification(pub u64);

#[derive(Copy, Clone, Debug)]
pub struct UnsubscriptionNotification(pub SubscriptionId);

#[derive(Copy, Clone, Debug)]
pub struct ProgressiveNotification(pub u64);

impl TryFrom<Notification> for ProgressiveNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::Progressive(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

impl TryFrom<Notification> for UnsubscriptionNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::Unsubscription(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionOkNotification {
    pub session_id: Bytes,
    pub request_limit: usize,
    pub keep_alive: usize,
    pub control_link: Bytes,
}

#[derive(Copy, Clone, Debug)]
pub struct SyncNotification(usize);

impl TryFrom<Notification> for ConnectionOkNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::ConnectionOk(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

impl ConnectionOkNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        Ok(Self {
            session_id: next_bytes("session_id", buffer, parts)?,
            request_limit: parse_next("request_limit", parts)?,
            keep_alive: parse_next("keep_alive", parts)?,
            control_link: next_bytes("control_link", buffer, parts)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SessionCreationFailedNotification {
    pub error_code: usize,
    pub error_message: Bytes,
}

impl SessionCreationFailedNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        Ok(Self {
            error_code: parse_next("error_code", parts)?,
            error_message: next_bytes("error_message", buffer, parts)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SessionEndedNotification {
    pub cause_code: usize,
    pub cause_message: Bytes,
}

impl TryFrom<Notification> for SessionEndedNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::SessionEnded(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

impl SessionEndedNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        Ok(Self {
            cause_code: parse_next("cause_code", parts)?,
            cause_message: next_bytes("cause_message", buffer, parts)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SubscriptionNotification {
    pub subscription_id: SubscriptionId,
    pub num_items: usize,
    pub num_fields: usize,
}

impl TryFrom<Notification> for SubscriptionNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::Subscription(notification) = value {
            Ok(notification.clone())
        } else {
            Err(value)
        }
    }
}

impl SubscriptionNotification {
    fn from_parts<'a>(
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        Ok(Self {
            subscription_id: parse_next("subscription_id", parts)?,
            num_items: parse_next("num_items", parts)?,
            num_fields: parse_next("num_fields", parts)?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ServerNameNotification(pub Bytes);

#[derive(Clone, Debug)]
pub struct SubscriptionConfigurationNotification {
    pub subscription_id: SubscriptionId,
    pub max_frequency: f64,
    pub filtered: bool,
}

impl SubscriptionConfigurationNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        let subscription_id = parse_next("subscription_id", parts)?;

        let max_frequency = match unsafe { next_str_unchecked("max_frequency", parts)? } {
            "unlimited" => f64::INFINITY,
            other => other
                .parse::<f64>()
                .map_err(ParsingErrorCause::InvalidFloat)?,
        };

        let filtered_flag = parts
            .next()
            .ok_or(ParsingErrorCause::MissingArgument("filtered"))?;

        let filtered = match filtered_flag {
            b"filtered" => true,
            b"unfiltered" => false,
            other => {
                return Err(ParsingErrorCause::InvalidValue(
                    "filtered",
                    buffer.slice_ref(other),
                ))
            }
        };

        Ok(Self {
            subscription_id,
            max_frequency,
            filtered,
        })
    }
}

#[derive(Clone, Debug)]
pub struct UpdateNotification {
    pub subscription_id: SubscriptionId,
    pub item_index: usize,
    pub updates: Arc<[ValueUpdate]>,
}

impl TryFrom<Notification> for UpdateNotification {
    type Error = Notification;

    fn try_from(value: Notification) -> Result<Self, Self::Error> {
        if let Notification::Update(notification) = value {
            Ok(notification)
        } else {
            Err(value)
        }
    }
}

impl UpdateNotification {
    fn from_parts<'a>(
        buffer: &Bytes,
        parts: &mut impl Iterator<Item = &'a [u8]>,
    ) -> Result<Self, ParsingErrorCause> {
        let subscription_id = parse_next("subscription_id", parts)?;
        let item_index = parse_next("item_index", parts)?;
        let encoded = next_bytes("field_values", buffer, parts)?;

        Ok(Self {
            subscription_id,
            item_index,
            updates: parse_field_updates(encoded)?.into(),
        })
    }
}

fn parse_field_updates(bytes: Bytes) -> Result<Vec<ValueUpdate>, ParsingErrorCause> {
    let (mut i, mut j) = (0, find_field_terminator(&bytes, 0));
    let mut updates = Vec::new();
    while i <= bytes.len() {
        // note: i == bytes.len() works as i == j and so i..j is an empty interval (even if indices are invalid)
        match parse_field_update(bytes.slice(i..j))? {
            Either::Left(value) => updates.push(value),
            Either::Right(values) => updates.extend_from_slice(&values),
        }
        i = j + 1;
        j = find_field_terminator(&bytes, i);
    }
    Ok(updates)
}

fn find_field_terminator(bytes: &Bytes, start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'|' {
            break;
        }
        i += if bytes[i] == b'%' { 2 } else { 1 };
    }
    i
}

fn parse_field_update(
    bytes: Bytes,
) -> Result<Either<ValueUpdate, Box<[ValueUpdate]>>, ParsingErrorCause> {
    return match bytes.len() {
        0 => Ok(Either::Left(ValueUpdate::Unchanged)),
        1 if bytes[0] == b'#' => Ok(Either::Left(ValueUpdate::Changed(FieldValue::Null))),
        1 if bytes[0] == b'$' => Ok(Either::Left(ValueUpdate::Changed(FieldValue::Empty))),
        _ if bytes[0] == b'^' => {
            let num = unsafe { std::str::from_utf8_unchecked(&bytes[1..]) }
                .parse::<usize>()
                .map_err(ParsingErrorCause::InvalidInteger)?;
            Ok(Either::Right(
                std::iter::repeat(ValueUpdate::Unchanged)
                    .take(num)
                    .collect(),
            ))
        }
        _ => Ok(Either::Left(ValueUpdate::Changed(FieldValue::Value(
            percent_decode(bytes),
        )))),
    };
}

fn percent_decode(bytes: Bytes) -> Bytes {
    let mut buffer = Vec::new();
    let mut escaped = false;
    for byte in bytes {
        if escaped {
            buffer.push(byte);
            escaped = false;
        } else if byte == b'%' {
            escaped = true;
        } else {
            buffer.push(byte);
        }
    }
    buffer.into()
}

#[derive(Debug)]
pub enum ParsingErrorCause {
    Utf8Encoding(Utf8Error),
    MissingTag,
    InvalidTag(Bytes),
    MissingArgument(&'static str),
    InvalidInteger(ParseIntError),
    InvalidFloat(ParseFloatError),
    InvalidIp(AddrParseError),
    InvalidValue(&'static str, Bytes),
    InvalidSubscriptionId(ParseSubscriptionIdError),
}

impl From<Utf8Error> for ParsingErrorCause {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8Encoding(value)
    }
}

impl From<ParseIntError> for ParsingErrorCause {
    fn from(value: ParseIntError) -> Self {
        Self::InvalidInteger(value)
    }
}

impl From<ParseFloatError> for ParsingErrorCause {
    fn from(value: ParseFloatError) -> Self {
        Self::InvalidFloat(value)
    }
}

impl From<AddrParseError> for ParsingErrorCause {
    fn from(value: AddrParseError) -> Self {
        Self::InvalidIp(value)
    }
}

impl From<ParseSubscriptionIdError> for ParsingErrorCause {
    fn from(value: ParseSubscriptionIdError) -> Self {
        Self::InvalidSubscriptionId(value)
    }
}

#[derive(Debug)]
pub struct ParsingError {
    raw: Bytes,
    cause: ParsingErrorCause,
}

impl ParsingError {
    pub fn raw(&self) -> Bytes {
        self.raw.clone()
    }

    pub fn cause(&self) -> &ParsingErrorCause {
        &self.cause
    }
}

pub fn parse_notification(raw: Bytes) -> Result<Notification, ParsingError> {
    parse_notification_internal(raw.clone()).map_err(|cause| ParsingError { raw, cause })
}

fn parse_notification_internal(buffer: Bytes) -> Result<Notification, ParsingErrorCause> {
    let mut parts = buffer.split(|&b| b == b',');
    let tag = parts.next().ok_or(ParsingErrorCause::MissingTag)?;
    match tag {
        b"U" => UpdateNotification::from_parts(&buffer, &mut parts).map(Notification::Update),
        b"PROBE" => Ok(Notification::Probe(ProbeNotification)),
        b"CONOK" => ConnectionOkNotification::from_parts(&buffer, &mut parts)
            .map(Notification::ConnectionOk),
        b"CONERR" => SessionCreationFailedNotification::from_parts(&buffer, &mut parts)
            .map(Notification::SessionFailed),
        b"END" => SessionEndedNotification::from_parts(&buffer, &mut parts)
            .map(Notification::SessionEnded),
        b"REQOK" => parse_next("request_id", &mut parts)
            .map(RequestOkNotification)
            .map(Notification::RequestOk),
        b"REQERR" => {
            RequestErrorNotification::from_parts(&buffer, parts).map(Notification::RequestError)
        }
        b"ERROR" => ErrorNotification::from_parts(&buffer, parts).map(Notification::Error),
        b"SERVNAME" => next_bytes("server_name", &buffer, &mut parts)
            .map(ServerNameNotification)
            .map(Notification::ServerName),
        b"CLIENTIP" => parse_next("ip", &mut parts)
            .map(ClientIpNotification)
            .map(Notification::ClientIp),
        b"NOOP" => next_bytes("msg", &buffer, &mut parts)
            .map(NoopNotification)
            .map(Notification::Noop),
        b"CONS" => {
            ConstrainNotification::from_parts(&buffer, &mut parts).map(Notification::Constrain)
        }
        b"LOOP" => parse_next("expected_delay", &mut parts)
            .map(LoopNotification)
            .map(Notification::Loop),
        b"SUBOK" => {
            SubscriptionNotification::from_parts(&mut parts).map(Notification::Subscription)
        }
        b"CONF" => SubscriptionConfigurationNotification::from_parts(&buffer, &mut parts)
            .map(Notification::SubscriptionConfiguration),
        b"SYNC" => parse_next("seconds_since_initial_header", &mut parts)
            .map(SyncNotification)
            .map(Notification::Sync),
        b"UNSUB" => parse_next("subscription_id", &mut parts)
            .map(UnsubscriptionNotification)
            .map(Notification::Unsubscription),
        b"PROG" => parse_next("progressive", &mut parts)
            .map(ProgressiveNotification)
            .map(Notification::Progressive),
        tag => Err(ParsingErrorCause::InvalidTag(buffer.slice_ref(tag))),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::notification::{FieldValue, ValueUpdate};

    use super::parse_field_updates;

    #[test]
    fn parse_field_updates_1() {
        let update = Bytes::from_static(b"20:00:33|3.04|0.0|2.41|3.67|3.03|3.04|#|#|$");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("20:00:33".into())),
                ValueUpdate::Changed(FieldValue::Value("3.04".into())),
                ValueUpdate::Changed(FieldValue::Value("0.0".into())),
                ValueUpdate::Changed(FieldValue::Value("2.41".into())),
                ValueUpdate::Changed(FieldValue::Value("3.67".into())),
                ValueUpdate::Changed(FieldValue::Value("3.03".into())),
                ValueUpdate::Changed(FieldValue::Value("3.04".into())),
                ValueUpdate::Changed(FieldValue::Null),
                ValueUpdate::Changed(FieldValue::Null),
                ValueUpdate::Changed(FieldValue::Empty)
            ]
        );
    }

    #[test]
    fn parse_field_updates_2() {
        let update = Bytes::from_static(b"20:00:54|3.07|0.98|||3.06|3.07|||Suspended");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("20:00:54".into())),
                ValueUpdate::Changed(FieldValue::Value("3.07".into())),
                ValueUpdate::Changed(FieldValue::Value("0.98".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Changed(FieldValue::Value("3.06".into())),
                ValueUpdate::Changed(FieldValue::Value("3.07".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Changed(FieldValue::Value("Suspended".into()))
            ]
        );
    }

    #[test]
    fn parse_field_updates_3() {
        let update = Bytes::from_static(b"20:04:16|3.02|-0.65|||3.01|3.02|||$");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("20:04:16".into())),
                ValueUpdate::Changed(FieldValue::Value("3.02".into())),
                ValueUpdate::Changed(FieldValue::Value("-0.65".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Changed(FieldValue::Value("3.01".into())),
                ValueUpdate::Changed(FieldValue::Value("3.02".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Changed(FieldValue::Empty)
            ]
        );
    }

    #[test]
    fn parse_field_updates_4() {
        let update = Bytes::from_static(b"20:04:40|^4|3.02|3.03|||");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("20:04:40".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Changed(FieldValue::Value("3.02".into())),
                ValueUpdate::Changed(FieldValue::Value("3.03".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged
            ]
        );
    }

    #[test]
    fn parse_field_updates_5() {
        let update = Bytes::from_static(b"20:06:10|3.05|0.32|^7");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("20:06:10".into())),
                ValueUpdate::Changed(FieldValue::Value("3.05".into())),
                ValueUpdate::Changed(FieldValue::Value("0.32".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged
            ]
        );
    }

    #[test]
    fn parse_field_updates_6() {
        let update = Bytes::from_static(b"%%10%%|%%3.05%%+|0.32|^7");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("%10%".into())),
                ValueUpdate::Changed(FieldValue::Value("%3.05%+".into())),
                ValueUpdate::Changed(FieldValue::Value("0.32".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged
            ]
        );
    }

    #[test]
    fn parse_field_updates_7() {
        let update = Bytes::from_static(b"%|10%||%$3.05%#+|0.32|^7");
        let res = parse_field_updates(update).unwrap();
        assert_eq!(
            res,
            &[
                ValueUpdate::Changed(FieldValue::Value("|10|".into())),
                ValueUpdate::Changed(FieldValue::Value("$3.05#+".into())),
                ValueUpdate::Changed(FieldValue::Value("0.32".into())),
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged,
                ValueUpdate::Unchanged
            ]
        );
    }
}
