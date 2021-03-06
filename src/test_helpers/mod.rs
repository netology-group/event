use async_std::prelude::*;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde_json::json;
use svc_agent::{
    mqtt::{IncomingEventProperties, IncomingRequestProperties, IncomingResponseProperties},
    AgentId,
};
use uuid::Uuid;

use crate::app::endpoint::{EventHandler, RequestHandler, ResponseHandler};
use crate::app::error::Error as AppError;
use crate::app::message_handler::MessageStream;
use crate::app::API_VERSION;

use self::agent::TestAgent;
use self::context::TestContext;
use self::outgoing_envelope::{
    OutgoingEnvelope, OutgoingEnvelopeProperties, OutgoingEventProperties,
    OutgoingRequestProperties, OutgoingResponseProperties,
};

///////////////////////////////////////////////////////////////////////////////

pub(crate) const SVC_AUDIENCE: &'static str = "dev.svc.example.org";
pub(crate) const USR_AUDIENCE: &'static str = "dev.usr.example.org";

pub(crate) async fn handle_request<H: RequestHandler>(
    context: &mut TestContext,
    agent: &TestAgent,
    payload: H::Payload,
) -> Result<Vec<OutgoingEnvelope>, AppError> {
    let reqp = build_reqp(agent.agent_id(), "ignore");
    let messages = H::handle(context, payload, &reqp).await?;
    Ok(parse_messages(messages).await)
}

pub(crate) async fn handle_response<H: ResponseHandler>(
    context: &mut TestContext,
    agent: &TestAgent,
    payload: H::Payload,
    corr_data: &H::CorrelationData,
) -> Result<Vec<OutgoingEnvelope>, AppError> {
    let respp = build_respp(agent.agent_id());
    let messages = H::handle(context, payload, &respp, corr_data).await?;
    Ok(parse_messages(messages).await)
}

pub(crate) async fn handle_event<H: EventHandler>(
    context: &mut TestContext,
    agent: &TestAgent,
    payload: H::Payload,
) -> Result<Vec<OutgoingEnvelope>, AppError> {
    let evp = build_evp(agent.agent_id(), "ignore");
    let messages = H::handle(context, payload, &evp).await?;
    Ok(parse_messages(messages).await)
}

async fn parse_messages(mut messages: MessageStream) -> Vec<OutgoingEnvelope> {
    let mut parsed_messages = vec![];

    while let Some(message) = messages.next().await {
        let dump = message
            .into_dump(TestAgent::new("alpha", "event", SVC_AUDIENCE).address())
            .expect("Failed to dump outgoing message");

        let mut parsed_message = serde_json::from_str::<OutgoingEnvelope>(dump.payload())
            .expect("Failed to parse dumped message");

        parsed_message.set_topic(dump.topic());
        parsed_messages.push(parsed_message);
    }

    parsed_messages
}

pub(crate) fn find_event<P>(messages: &[OutgoingEnvelope]) -> (P, &OutgoingEventProperties, &str)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
            return (message.payload::<P>(), evp, message.topic());
        }
    }

    panic!("Event not found");
}

pub(crate) fn find_event_by_predicate<P, F>(
    messages: &[OutgoingEnvelope],
    f: F,
) -> Option<(P, &OutgoingEventProperties, &str)>
where
    P: DeserializeOwned,
    F: Fn(&OutgoingEventProperties) -> bool,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
            if f(evp) {
                return Some((message.payload::<P>(), evp, message.topic()));
            }
        }
    }

    return None;
}

pub(crate) fn find_response<P>(
    messages: &[OutgoingEnvelope],
) -> (P, &OutgoingResponseProperties, &str)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Response(respp) = message.properties() {
            return (message.payload::<P>(), respp, message.topic());
        }
    }

    panic!("Response not found");
}

pub(crate) fn find_request<P>(
    messages: &[OutgoingEnvelope],
) -> (P, &OutgoingRequestProperties, &str)
where
    P: DeserializeOwned,
{
    for message in messages {
        if let OutgoingEnvelopeProperties::Request(reqp) = message.properties() {
            return (message.payload::<P>(), reqp, message.topic());
        }
    }

    panic!("Request not found");
}

pub(crate) fn build_reqp(agent_id: &AgentId, method: &str) -> IncomingRequestProperties {
    let now = Utc::now().timestamp_millis().to_string();

    let reqp_json = json!({
        "type": "request",
        "correlation_data": "123456789",
        "agent_id": agent_id,
        "connection_mode": "default",
        "connection_version": "v2",
        "method": method,
        "response_topic": format!(
            "agents/{}/api/{}/in/event.{}",
            agent_id, API_VERSION, SVC_AUDIENCE
        ),
        "broker_agent_id": format!("alpha.mqtt-gateway.{}", SVC_AUDIENCE),
        "broker_timestamp": now,
        "broker_processing_timestamp": now,
        "broker_initial_processing_timestamp": now,
        "tracking_id": format!("{}.{}.{}", Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()),
        "session_tracking_label": format!(
            "{}.{} {}.{}",
            Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()
        ),
    });

    serde_json::from_value::<IncomingRequestProperties>(reqp_json).expect("Failed to parse reqp")
}

pub(crate) fn build_respp(agent_id: &AgentId) -> IncomingResponseProperties {
    let now = Utc::now().timestamp_millis().to_string();

    let respp_json = json!({
        "type": "response",
        "status": "200",
        "correlation_data": "ignore",
        "agent_id": agent_id,
        "connection_mode": "default",
        "connection_version": "v2",
        "broker_agent_id": format!("alpha.mqtt-gateway.{}", SVC_AUDIENCE),
        "broker_timestamp": now,
        "broker_processing_timestamp": now,
        "broker_initial_processing_timestamp": now,
        "tracking_id": format!("{}.{}.{}", Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()),
        "session_tracking_label": format!(
            "{}.{} {}.{}",
            Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()
        ),
    });

    serde_json::from_value::<IncomingResponseProperties>(respp_json).expect("Failed to parse respp")
}

pub(crate) fn build_evp(agent_id: &AgentId, label: &str) -> IncomingEventProperties {
    let now = Utc::now().timestamp_millis().to_string();

    let evp_json = json!({
        "type": "event",
        "label": label,
        "agent_id": agent_id,
        "connection_mode": "default",
        "connection_version": "v2",
        "broker_agent_id": format!("alpha.mqtt-gateway.{}", SVC_AUDIENCE),
        "broker_timestamp": now,
        "broker_processing_timestamp": now,
        "broker_initial_processing_timestamp": now,
        "tracking_id": format!("{}.{}.{}", Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()),
        "session_tracking_label": format!(
            "{}.{} {}.{}",
            Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()
        ),
    });

    serde_json::from_value::<IncomingEventProperties>(evp_json).expect("Failed to parse evp")
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod prelude {
    #[allow(unused_imports)]
    pub(crate) use crate::app::context::GlobalContext;

    #[allow(unused_imports)]
    pub(crate) use super::{
        agent::TestAgent,
        authz::{DbBanTestAuthz, TestAuthz},
        build_evp, build_reqp, build_respp,
        context::TestContext,
        db::{test_db_ban_callback, TestDb},
        factory, find_event, find_event_by_predicate, find_request, find_response, handle_event,
        handle_request, handle_response, shared_helpers, SVC_AUDIENCE, USR_AUDIENCE,
    };
}

pub(crate) mod agent;
pub(crate) mod authz;
pub(crate) mod context;
pub(crate) mod db;
pub(crate) mod factory;
pub(crate) mod outgoing_envelope;
pub(crate) mod shared_helpers;
