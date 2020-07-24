use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingRequest, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};
use svc_authn::Authenticable;
use svc_error::{extension::sentry, Error as SvcError};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::operations::recalculate_chat_notifications;
use crate::db::chat_notification::Object as ChatNotification;
use crate::db::room::FindQuery as RoomFindQuery;

///////////////////////////////////////////////////////////////////////////////

const MQTT_GW_API_VERSION: &str = "v1";

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateRequest {
    room_id: Uuid,
    last_seen_id: Uuid,
}

pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;
    const ERROR_TITLE: &'static str = "Failed to update last_seen_id in room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let conn = context.db().get()?;

            RoomFindQuery::new(payload.room_id)
                .execute(&conn)?
                .ok_or_else(|| format!("the room = '{}' is not found", payload.room_id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room events listing.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "events"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "list")
            .await?;

        let notification = {
            let mut task_finished = false;
            let db = context.db().clone();
            let to = reqp.as_agent_id().as_account_id().to_owned();
            let event_type = context.config().notifications_event_type.clone();

            stream::from_fn(move || {
                if let Some(ref event_type) = event_type {
                    if task_finished {
                        return None;
                    }

                    match recalculate_chat_notifications(
                        &db,
                        &room,
                        payload.last_seen_id,
                        &to,
                        event_type,
                    ) {
                        Ok(vec) => {
                            let vec = vec
                                .into_iter()
                                .map(|notif| {
                                    let timing = ShortTermTimingProperties::new(Utc::now());
                                    let props = OutgoingEventProperties::new(
                                        "chat_notifications.update",
                                        timing,
                                    );
                                    let event = OutgoingEvent::multicast(notif, props, &to);

                                    task_finished = true;
                                    Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                                })
                                .collect::<Vec<_>>();
                            Some(stream::from_iter(vec))
                        }
                        Err(err) => {
                            error!(
                                "Recalculate chat notifications job failed for (room_id, last_seen_id) = ('{}', '{}'): {}",
                                room.id(),
                                payload.last_seen_id,
                                err
                            );

                            let error = SvcError::builder()
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .kind(
                                    "chat_notifications.update",
                                    "Failed to update notification counters",
                                )
                                .detail(&err.to_string())
                                .build();

                            sentry::send(error.clone()).unwrap_or_else(|err| {
                                warn!("Error sending error to Sentry: {}", err)
                            });

                            None
                        }
                    }
                } else {
                    None
                }
            })
        };

        let notification = notification.flatten();

        let response = helpers::build_response(
            ResponseStatus::OK,
            json!("{}"),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        Ok(Box::new(
            stream::from_iter(vec![response]).chain(notification),
        ))
    }
}

#[derive(Debug, Serialize)]
struct SubscriptionRequest {
    subject: AgentId,
    object: Vec<String>,
}

impl SubscriptionRequest {
    fn new(subject: AgentId, object: Vec<&str>) -> Self {
        Self {
            subject,
            object: object.iter().map(|&s| s.into()).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct SubscribeRequest {}

pub(crate) struct SubscribeHandler;

#[async_trait]
impl RequestHandler for SubscribeHandler {
    type Payload = SubscribeRequest;
    const ERROR_TITLE: &'static str = "Failed to subscribe to chat notifications";

    async fn handle<C: Context>(
        context: &C,
        _payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let account_id = reqp.as_agent_id().as_account_id().to_string();
        let object = vec!["chat_notifications", &account_id];

        // Send dynamic subscription creation request to the broker.
        let payload = SubscriptionRequest::new(reqp.as_agent_id().to_owned(), object);

        let short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);

        let props = reqp.to_request(
            "subscription.create",
            reqp.response_topic(),
            reqp.correlation_data(),
            short_term_timing,
        );

        let notification = {
            let db = context.db().clone();
            let to = reqp.as_agent_id().as_account_id().to_owned();
            let event_type_present = context.config().notifications_event_type.is_some();

            stream::from_fn(move || {
                if event_type_present {
                    let query = crate::db::chat_notification::ListQuery::new(&to);

                    let get_notifications = || -> anyhow::Result<Vec<ChatNotification>> {
                        let conn = db.get()?;
                        query.execute(&conn).map_err(|e| e.into())
                    };

                    let stream = match get_notifications() {
                        Ok(notifs) => {
                            let vec = notifs
                                .into_iter()
                                .map(|notif| {
                                    let timing = ShortTermTimingProperties::new(Utc::now());
                                    let props = OutgoingEventProperties::new(
                                        "chat_notifications.update",
                                        timing,
                                    );
                                    let event = OutgoingEvent::multicast(notif, props, &to);

                                    Box::new(event) as Box<dyn IntoPublishableMessage + Send>
                                })
                                .collect::<Vec<_>>();
                            Some(stream::from_iter(vec))
                        }
                        Err(err) => {
                            error!(
                                "Failed to fetch chat notifications for user = '{}': {}",
                                to, err
                            );

                            let error = SvcError::builder()
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .kind(
                                    "chat_notifications.subscribe",
                                    "Failed to send initial counters state",
                                )
                                .detail(&err.to_string())
                                .build();

                            sentry::send(error.clone()).unwrap_or_else(|err| {
                                warn!("Error sending error to Sentry: {}", err)
                            });

                            None
                        }
                    };

                    stream
                } else {
                    None
                }
            })
        };

        let notification = notification.flatten();

        // FIXME: It looks like sending a request to the client but the broker intercepts it
        //        creates a subscription and replaces the request with the response.
        //        This is kind of ugly but it guaranties that the request will be processed by
        //        the broker node where the client is connected to. We need that because
        //        the request changes local state on that node.
        //        A better solution will be possible after resolution of this issue:
        //        https://github.com/vernemq/vernemq/issues/1326.
        //        Then we won't need the local state on the broker at all and will be able
        //        to send a multicast request to the broker.
        let outgoing_request = OutgoingRequest::unicast(payload, props, reqp, MQTT_GW_API_VERSION);
        let boxed_request = Box::new(outgoing_request) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_request).chain(notification)))
    }
}
