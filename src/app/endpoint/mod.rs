use std::result::Result as StdResult;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use svc_agent::mqtt::{
    IncomingEvent, IncomingEventProperties, IncomingRequest, IncomingRequestProperties,
};
use svc_error::Error as SvcError;

use crate::app::context::Context;
pub(self) use crate::app::message_handler::MessageStream;
use crate::app::message_handler::{EventEnvelopeHandler, RequestEnvelopeHandler};

///////////////////////////////////////////////////////////////////////////////

pub(crate) type Result = StdResult<MessageStream, SvcError>;

#[async_trait]
pub(crate) trait RequestHandler {
    type Payload: Send + DeserializeOwned;
    const ERROR_TITLE: &'static str;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result;
}

macro_rules! request_routes {
    ($($m: pat => $h: ty),*) => {
        pub(crate) async fn route_request<C: Context>(
            context: &C,
            request: &IncomingRequest<String>,
            start_timestamp: DateTime<Utc>,
        ) -> Option<MessageStream> {
            match request.properties().method() {
                $(
                    $m => Some(
                        <$h>::handle_envelope::<C>(context, request, start_timestamp).await
                    ),
                )*
                _ => None,
            }
        }
    }
}

// Request routes configuration: method => RequestHandler
request_routes!(
    "agent.list" => agent::ListHandler,
    "change.create" => change::CreateHandler,
    "change.delete" => change::DeleteHandler,
    "change.list" => change::ListHandler,
    "edition.commit" => edition::CommitHandler,
    "edition.create" => edition::CreateHandler,
    "edition.list" => edition::ListHandler,
    "edition.delete" => edition::DeleteHandler,
    "event.create" => event::CreateHandler,
    "event.list" => event::ListHandler,
    "room.adjust" => room::AdjustHandler,
    "room.create" => room::CreateHandler,
    "room.enter" => room::EnterHandler,
    "room.leave" => room::LeaveHandler,
    "room.read" => room::ReadHandler,
    "room.update" => room::UpdateHandler,
    "state.read" => state::ReadHandler,
    "chat_notifications.update" => chat_notifications::UpdateHandler,
    "chat_notifications.subscribe" => chat_notifications::SubscribeHandler
);

///////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub(crate) trait EventHandler {
    type Payload: Send + DeserializeOwned;

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        evp: &IncomingEventProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result;
}

macro_rules! event_routes {
    ($($l: pat => $h: ty),*) => {
        #[allow(unused_variables)]
        pub(crate) async fn route_event<C: Context>(
            context: &C,
            event: &IncomingEvent<String>,
            start_timestamp: DateTime<Utc>,
        ) -> Option<MessageStream> {
            match event.properties().label() {
                $(
                    Some($l) => Some(
                        <$h>::handle_envelope::<C>(context, event, start_timestamp).await
                    ),
                )*
                _ => None,
            }
        }
    }
}

// Event routes configuration: label => EventHandler
event_routes!(
    "metric.pull" => metric::PullHandler,
    "subscription.delete" => subscription::DeleteHandler,
    "subscription.create" => subscription::CreateHandler
);

///////////////////////////////////////////////////////////////////////////////

mod agent;
mod change;
mod chat_notifications;
mod edition;
mod event;
pub(self) mod helpers;
mod metric;
mod room;
mod state;
mod subscription;

pub(self) mod prelude {
    pub(super) use super::{helpers, EventHandler, RequestHandler, Result};
    pub(super) use crate::app::message_handler::SvcErrorSugar;
}
