use std::future::Future;
use std::pin::Pin;

use anyhow::anyhow;
use async_std::prelude::*;
use async_std::stream::{self, Stream};
use chrono::{DateTime, Utc};
use futures_util::pin_mut;
use log::{error, warn};
use svc_agent::mqtt::{
    Agent, IncomingEvent, IncomingMessage, IncomingRequest, IncomingRequestProperties,
    IntoPublishableMessage, OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
};
use svc_error::{extension::sentry, Error as SvcError};

use crate::app::{context::Context, endpoint, API_VERSION};

pub(crate) type MessageStream =
    Box<dyn Stream<Item = Box<dyn IntoPublishableMessage + Send>> + Send + Unpin>;

pub(crate) struct MessageHandler<C: Context> {
    agent: Agent,
    context: C,
}

impl<C: Context + Sync> MessageHandler<C> {
    pub(crate) fn new(agent: Agent, context: C) -> Self {
        Self { agent, context }
    }

    pub(crate) fn agent(&self) -> &Agent {
        &self.agent
    }

    pub(crate) fn context(&self) -> &C {
        &self.context
    }

    pub(crate) async fn handle(&self, message: &Result<IncomingMessage<String>, String>) {
        match message {
            Ok(ref message) => {
                if let Err(err) = self.handle_message(message).await {
                    error!("Error processing a message = '{:?}': {}", message, err);

                    sentry::send(err)
                        .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
                }
            }
            Err(e) => {
                error!("Error processing a message = '{:?}': {}", message, e,);

                let svc_error = SvcError::builder()
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                    .kind("message_handling_failed", "Message handling failed")
                    .detail(&e.to_string())
                    .build();

                sentry::send(svc_error)
                    .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
            }
        }
    }

    async fn handle_message(&self, message: &IncomingMessage<String>) -> Result<(), SvcError> {
        let start_timestamp = Utc::now();

        match message {
            IncomingMessage::Request(req) => self.handle_request(req, start_timestamp).await,
            IncomingMessage::Response(_) => {
                // This service doesn't send any requests to other services so we don't
                // expect any responses.
                warn!("Unexpected incoming response message");
                Ok(())
            }
            IncomingMessage::Event(ev) => self.handle_event(ev, start_timestamp).await,
        }
    }

    async fn handle_request(
        &self,
        request: &IncomingRequest<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Result<(), SvcError> {
        let outgoing_message_stream =
            endpoint::route_request(&self.context, request, start_timestamp)
                .await
                .unwrap_or_else(|| {
                    error_response(
                        ResponseStatus::METHOD_NOT_ALLOWED,
                        "unknown_method",
                        "Unknown method",
                        &format!("Unknown method '{}'", request.properties().method()),
                        request.properties(),
                        start_timestamp,
                    )
                });

        self.publish_outgoing_messages(outgoing_message_stream)
            .await
    }

    async fn handle_event(
        &self,
        event: &IncomingEvent<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Result<(), SvcError> {
        match event.properties().label() {
            Some(label) => {
                let outgoing_message_stream =
                    endpoint::route_event(&self.context, event, start_timestamp)
                        .await
                        .unwrap_or_else(|| {
                            warn!("Unexpected event with label = '{}'", label);
                            Box::new(stream::empty())
                        });

                self.publish_outgoing_messages(outgoing_message_stream)
                    .await
            }
            None => {
                warn!("Got event with missing label");
                Ok(())
            }
        }
    }

    async fn publish_outgoing_messages(
        &self,
        message_stream: MessageStream,
    ) -> Result<(), SvcError> {
        let mut agent = self.agent.clone();
        pin_mut!(message_stream);

        while let Some(message) = message_stream.next().await {
            publish_message(&mut agent, message)?;
        }

        Ok(())
    }
}

fn error_response(
    status: ResponseStatus,
    kind: &str,
    title: &str,
    detail: &str,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> MessageStream {
    let err = SvcError::builder()
        .status(status)
        .kind(kind, title)
        .detail(detail)
        .build();

    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = reqp.to_response(status, timing);
    let resp = OutgoingResponse::unicast(err, props, reqp, API_VERSION);
    Box::new(stream::once(
        Box::new(resp) as Box<dyn IntoPublishableMessage + Send>
    ))
}

pub(crate) fn publish_message(
    agent: &mut Agent,
    message: Box<dyn IntoPublishableMessage>,
) -> Result<(), SvcError> {
    use crate::app::error::{AppError, ErrorExt};

    agent
        .publish_publishable(message)
        .map_err(|err| anyhow!("Failed to publish message: {}", err))
        .error(AppError::PublishFailed)
}

///////////////////////////////////////////////////////////////////////////////

// These auto-traits are being defined on all request/event handlers.
// They do parsing of the envelope and payload, call the handler and perform error handling.
// So we don't implement these generic things in each handler.
// We just need to specify the payload type and specific logic.

pub(crate) trait RequestEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        request: &'async_trait IncomingRequest<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// Can't use `#[async_trait]` macro here because it's not smart enough to add `'async_trait`
// lifetime to `H` type parameter. The creepy stuff around the actual implementation is what
// this macro expands to based on https://github.com/dtolnay/async-trait#explanation.
impl<'async_trait, H: 'async_trait + Sync + endpoint::RequestHandler>
    RequestEnvelopeHandler<'async_trait> for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        request: &'async_trait IncomingRequest<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>
    where
        Self: Sync + 'async_trait,
    {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::RequestHandler, C: Context>(
            context: &C,
            request: &IncomingRequest<String>,
            start_timestamp: DateTime<Utc>,
        ) -> MessageStream {
            // Parse the envelope with the payload type specified in the handler.
            let payload = IncomingRequest::convert_payload::<H::Payload>(request);
            let reqp = request.properties();
            match payload {
                // Call handler.
                Ok(payload) => {
                    H::handle(context, payload, reqp, start_timestamp)
                        .await
                        .unwrap_or_else(|svc_error| {
                            error!(
                                "Failed to handle request with method = '{}': {}",
                                reqp.method(),
                                svc_error
                            );

                            sentry::send(svc_error.clone()).unwrap_or_else(|err| {
                                warn!("Error sending error to Sentry: {}", err)
                            });

                            // Handler returned an error => 422.
                            error_response(
                                svc_error.status_code(),
                                svc_error.kind(),
                                svc_error.title(),
                                &svc_error.to_string(),
                                reqp,
                                start_timestamp,
                            )
                        })
                }
                // Bad envelope or payload format => 400.
                Err(err) => error_response(
                    ResponseStatus::BAD_REQUEST,
                    "invalid_payload",
                    "Invalid payload",
                    &err.to_string(),
                    reqp,
                    start_timestamp,
                ),
            }
        }

        Box::pin(handle_envelope::<H, C>(context, request, start_timestamp))
    }
}

pub(crate) trait EventEnvelopeHandler<'async_trait> {
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        envelope: &'async_trait IncomingEvent<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>>;
}

// This is the same as with the above.
impl<'async_trait, H: 'async_trait + endpoint::EventHandler> EventEnvelopeHandler<'async_trait>
    for H
{
    fn handle_envelope<C: Context>(
        context: &'async_trait C,
        event: &'async_trait IncomingEvent<String>,
        start_timestamp: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = MessageStream> + Send + 'async_trait>> {
        // The actual implementation.
        async fn handle_envelope<H: endpoint::EventHandler, C: Context>(
            context: &C,
            event: &IncomingEvent<String>,
            start_timestamp: DateTime<Utc>,
        ) -> MessageStream {
            // Parse event envelope with the payload from the handler.
            let payload = IncomingEvent::convert_payload::<H::Payload>(event);
            let evp = event.properties();

            match payload {
                // Call handler.
                Ok(payload) => H::handle(context, payload, evp, start_timestamp)
                    .await
                    .unwrap_or_else(|svc_error| {
                        // Handler returned an error.
                        if let Some(label) = evp.label() {
                            error!(
                                "Failed to handle event with label = '{}': {}",
                                label, svc_error
                            );

                            sentry::send(svc_error).unwrap_or_else(|err| {
                                warn!("Error sending error to Sentry: {}", err)
                            });
                        }

                        Box::new(stream::empty())
                    }),
                Err(err) => {
                    // Bad envelope or payload format.
                    if let Some(label) = evp.label() {
                        error!("Failed to parse event with label = '{}': {}", label, err);
                    }

                    Box::new(stream::empty())
                }
            }
        }

        Box::pin(handle_envelope::<H, C>(context, event, start_timestamp))
    }
}
