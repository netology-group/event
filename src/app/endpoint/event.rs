use std::ops::Bound;

use async_std::stream;
use async_std::stream::StreamExt;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, warn};
use serde_derive::Deserialize;
use serde_json::Value as JsonValue;
use svc_agent::Authenticable;
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        ResponseStatus, ShortTermTimingProperties,
    },
    Addressable,
};
use svc_error::{extension::sentry, Error as SvcError};
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::operations::increment_chat_notifications;
use crate::db;
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    room_id: Uuid,
    #[serde(rename = "type")]
    kind: String,
    set: Option<String>,
    label: Option<String>,
    data: JsonValue,
    #[serde(default = "CreateRequest::default_is_claim")]
    is_claim: bool,
    #[serde(default = "CreateRequest::default_is_persistent")]
    is_persistent: bool,
    #[serde(default = "CreateRequest::default_priority")]
    priority: i32,
}

impl CreateRequest {
    fn default_is_claim() -> bool {
        false
    }

    fn default_is_persistent() -> bool {
        true
    }

    fn default_priority() -> i32 {
        crate::db::chat_notification::DEFAULT_PRIORITY
    }
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create event";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let (room, author) = {
            let conn = context.db().get()?;

            // Check whether the room exists and open.
            let room = db::room::FindQuery::new(payload.room_id)
                .time(db::room::now())
                .execute(&conn)?
                .ok_or_else(|| format!("the room = '{}' is not found or closed", payload.room_id))
                .status(ResponseStatus::NOT_FOUND)?;

            let author = match payload {
                // Get author of the original event with the same label if applicable.
                CreateRequest {
                    set: Some(ref set),
                    label: Some(ref label),
                    ..
                } => db::event::OriginalEventQuery::new(room.id(), set, label)
                    .execute(&conn)?
                    .map(|original_event| original_event.created_by().as_account_id().to_string()),
                _ => None,
            }
            .unwrap_or_else(|| {
                // If set & label are not given or there're no events for them use current account.
                reqp.as_account_id().to_string()
            });

            (room, author)
        };

        // Authorize event creation on tenant with cache.
        let room_id = room.id().to_string();
        let key = if payload.is_claim { "claims" } else { "events" };
        let object = vec!["rooms", &room_id, key, &payload.kind, "authors", &author];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object.clone(), "create")
            .await?;

        // Calculate occurrence date.
        let occurred_at = match room.time() {
            (Bound::Included(opened_at), _) => (Utc::now() - opened_at.to_owned())
                .num_nanoseconds()
                .unwrap_or(std::i64::MAX),
            _ => {
                return Err(format!("invalid time for room = '{}'", room.id()))
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY);
            }
        };

        let event = if payload.is_persistent {
            // Insert event into the DB.
            let mut query = db::event::InsertQuery::new(
                room.id(),
                &payload.kind,
                &payload.data,
                occurred_at,
                reqp.as_agent_id(),
            )
            .priority(payload.priority);

            if let Some(ref set) = payload.set {
                query = query.set(set);
            }

            if let Some(ref label) = payload.label {
                query = query.label(label);
            }

            {
                let conn = context.db().get()?;

                query
                    .execute(&conn)
                    .map_err(|err| format!("failed to create event: {}", err))
                    .status(ResponseStatus::UNPROCESSABLE_ENTITY)?
            }
        } else {
            // Build transient event.
            let mut builder = db::event::Builder::new()
                .room_id(payload.room_id)
                .kind(&payload.kind)
                .data(&payload.data)
                .occurred_at(occurred_at)
                .created_by(reqp.as_agent_id())
                .priority(payload.priority);

            if let Some(ref set) = payload.set {
                builder = builder.set(set)
            }

            if let Some(ref label) = payload.label {
                builder = builder.label(label)
            }

            builder
                .build()
                .map_err(|err| format!("Error building transient event: {}", err,))
                .status(ResponseStatus::UNPROCESSABLE_ENTITY)?
        };

        let notification = {
            let mut task_finished = false;
            let event = event.clone();
            let db = context.db().clone();
            let is_tracked_message = if payload.set.is_some()
                && Some(payload.kind) == context.config().notifications_event_type
            {
                true
            } else {
                false
            };
            let to = reqp.as_agent_id().as_account_id().to_owned();

            stream::from_fn(move || {
                if is_tracked_message {
                    if task_finished {
                        return None;
                    }

                    match increment_chat_notifications(&db, &event) {
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
                                "Increment chat notifications jobs failed for (room_id, event_id) = ('{}', '{}'): {}",
                                event.room_id(),
                                event.id(),
                                err
                            );

                            let error = SvcError::builder()
                                .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                                .kind("event.create", "Failed to update notification counters")
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

        let mut messages = Vec::with_capacity(3);

        // Respond to the agent.
        messages.push(helpers::build_response(
            ResponseStatus::CREATED,
            event.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        ));

        // If the event is claim notify the tenant.
        if payload.is_claim {
            messages.push(helpers::build_notification(
                "event.create",
                &format!("audiences/{}/events", room.audience()),
                event.clone(),
                reqp,
                start_timestamp,
            ));
        }

        // Notify room subscribers.
        messages.push(helpers::build_notification(
            "event.create",
            &format!("rooms/{}/events", room.id()),
            event,
            reqp,
            start_timestamp,
        ));

        Ok(Box::new(stream::from_iter(messages).chain(notification)))
    }
}

///////////////////////////////////////////////////////////////////////////////

const MAX_LIMIT: i64 = 100;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequest {
    room_id: Uuid,
    #[serde(rename = "type")]
    kind: Option<String>,
    set: Option<String>,
    label: Option<String>,
    last_occurred_at: Option<i64>,
    #[serde(default)]
    direction: db::event::Direction,
    limit: Option<i64>,
}

pub(crate) struct ListHandler;

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;
    const ERROR_TITLE: &'static str = "Failed to list events";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        // Check whether the room exists.
        let room = {
            let conn = context.db().get()?;

            db::room::FindQuery::new(payload.room_id)
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

        // Retrieve events from the DB.
        let mut query = db::event::ListQuery::new().room_id(room.id());

        if let Some(ref kind) = payload.kind {
            query = query.kind(kind);
        }

        if let Some(ref set) = payload.set {
            query = query.set(set);
        }

        if let Some(ref label) = payload.label {
            query = query.label(label);
        }

        if let Some(last_occurred_at) = payload.last_occurred_at {
            query = query.last_occurred_at(last_occurred_at);
        }

        let events = {
            let conn = context.db().get()?;

            query
                .direction(payload.direction)
                .limit(std::cmp::min(
                    payload.limit.unwrap_or_else(|| MAX_LIMIT),
                    MAX_LIMIT,
                ))
                .execute(&conn)?
        };

        // Respond with events list.
        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            events,
            reqp,
            start_timestamp,
            Some(authz_time),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::db::chat_notification::DEFAULT_PRIORITY;
    use crate::db::event::{Direction, Object as Event};
    use crate::test_helpers::outgoing_envelope::OutgoingEnvelopeProperties;
    use crate::test_helpers::prelude::*;

    use super::*;

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn create_event() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room and put the agent online.
                let room = shared_helpers::insert_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Allow agent to create events of type `message` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let account_id = agent.account_id().to_string();

            let object = vec![
                "rooms",
                &room_id,
                "events",
                "message",
                "authors",
                &account_id,
            ];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request.
            let context = TestContext::new(db, authz);

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "hello" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect("Event creation failed");

            assert_eq!(messages.len(), 2);

            // Assert response.
            let (event, respp) = find_response::<Event>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(event.room_id(), room.id());
            assert_eq!(event.kind(), "message");
            assert_eq!(event.set(), "messages");
            assert_eq!(event.label(), Some("message-1"));
            assert_eq!(event.data(), &json!({ "text": "hello" }));

            // Assert notification.
            let (event, evp, topic) = find_event::<Event>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "event.create");
            assert_eq!(event.room_id(), room.id());
            assert_eq!(event.kind(), "message");
            assert_eq!(event.set(), "messages");
            assert_eq!(event.label(), Some("message-1"));
            assert_eq!(event.data(), &json!({ "text": "hello" }));
        });
    }

    #[test]
    fn create_next_event() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let original_author = TestAgent::new("web", "user123", USR_AUDIENCE);
            let agent = TestAgent::new("web", "moderator", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                let room = shared_helpers::insert_room(&conn);

                // Add an event to the room.
                factory::Event::new()
                    .room_id(room.id())
                    .kind("message")
                    .set("messages")
                    .label("message-1")
                    .data(&json!({ "text": "original text" }))
                    .occurred_at(1_000_000_000)
                    .created_by(&original_author.agent_id())
                    .insert(&conn);

                // Put the agent online.
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Allow agent to create events of type `message` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();

            // Should authorize with the author of the original event.
            let account_id = original_author.agent_id().as_account_id().to_string();

            let object = vec![
                "rooms",
                &room_id,
                "events",
                "message",
                "authors",
                &account_id,
            ];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request with the same set/label as existing event.
            let context = TestContext::new(db, authz);

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "modified text" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect("Event creation failed");

            // Assert response.
            let (event, respp) = find_response::<Event>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(event.created_by(), agent.agent_id());
        });
    }

    #[test]
    fn create_claim() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room and put the agent online.
                let room = shared_helpers::insert_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Allow agent to create claims of type `block` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let account_id = agent.account_id().to_string();

            let object = vec!["rooms", &room_id, "claims", "block", "authors", &account_id];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request.
            let context = TestContext::new(db, authz);

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("block"),
                set: Some(String::from("blocks")),
                label: Some(String::from("user-1")),
                data: json!({ "blocked": true }),
                is_claim: true,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect("Event creation failed");

            assert_eq!(messages.len(), 3);

            // Assert response.
            let (event, respp) = find_response::<Event>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(event.room_id(), room.id());
            assert_eq!(event.kind(), "block");
            assert_eq!(event.set(), "blocks");
            assert_eq!(event.label(), Some("user-1"));
            assert_eq!(event.data(), &json!({ "blocked": true }));

            // Assert tenant & room notifications.
            let mut has_tenant_notification = false;
            let mut has_room_notification = false;

            for message in messages {
                if let OutgoingEnvelopeProperties::Event(evp) = message.properties() {
                    let topic = message.topic();

                    if topic.ends_with(&format!("/audiences/{}/events", room.audience())) {
                        has_tenant_notification = true;
                    }

                    if topic.ends_with(&format!("/rooms/{}/events", room.id())) {
                        has_room_notification = true;
                    }

                    assert_eq!(evp.label(), "event.create");

                    let event = message.payload::<Event>();
                    assert_eq!(event.room_id(), room.id());
                    assert_eq!(event.kind(), "block");
                    assert_eq!(event.set(), "blocks");
                    assert_eq!(event.label(), Some("user-1"));
                    assert_eq!(event.data(), &json!({ "blocked": true }));
                }
            }

            assert_eq!(has_tenant_notification, true);
            assert_eq!(has_room_notification, true);
        });
    }

    #[test]
    fn create_transient_event() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room and put the agent online.
                let room = shared_helpers::insert_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Allow agent to create events of type `message` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let account_id = agent.account_id().to_string();

            let object = vec![
                "rooms",
                &room_id,
                "events",
                "cursor",
                "authors",
                &account_id,
            ];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request.
            let context = TestContext::new(db, authz);

            let data = json!({
                "agent_id": agent.agent_id().to_string(),
                "x": 123,
                "y": 456,
            });

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("cursor"),
                set: None,
                label: None,
                data: data.clone(),
                is_claim: false,
                is_persistent: false,
                priority: DEFAULT_PRIORITY,
            };

            let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect("Event creation failed");

            assert_eq!(messages.len(), 2);

            // Assert response.
            let (event, respp) = find_response::<Event>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::CREATED);
            assert_eq!(event.room_id(), room.id());
            assert_eq!(event.kind(), "cursor");
            assert_eq!(event.set(), "cursor");
            assert_eq!(event.label(), None);
            assert_eq!(event.data(), &data);

            // Assert notification.
            let (event, evp, topic) = find_event::<Event>(messages.as_slice());
            assert!(topic.ends_with(&format!("/rooms/{}/events", room.id())));
            assert_eq!(evp.label(), "event.create");
            assert_eq!(event.room_id(), room.id());
            assert_eq!(event.kind(), "cursor");
            assert_eq!(event.set(), "cursor");
            assert_eq!(event.label(), None);
            assert_eq!(event.data(), &data);
        });
    }

    #[test]
    fn create_event_not_authorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room and put the agent online.
                let room = shared_helpers::insert_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Make event.create request.
            let context = TestContext::new(db, TestAuthz::new());

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "hello" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let err = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect_err("Unexpected success on event creation");

            assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
        });
    }

    #[test]
    fn create_event_not_entered() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                shared_helpers::insert_room(&conn)
            };

            // Allow agent to create events of type `message` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let account_id = agent.account_id().to_string();

            let object = vec![
                "rooms",
                &room_id,
                "events",
                "message",
                "authors",
                &account_id,
            ];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request.
            let context = TestContext::new(db, authz);

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "hello" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect("Event creation failed");

            assert_eq!(messages.len(), 2);
        });
    }

    #[test]
    fn create_event_closed_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create closed room and put the agent online.
                let room = shared_helpers::insert_closed_room(&conn);
                shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                room
            };

            // Allow agent to create events of type `message` in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let account_id = agent.account_id().to_string();

            let object = vec![
                "rooms",
                &room_id,
                "events",
                "message",
                "authors",
                &account_id,
            ];

            authz.allow(agent.account_id(), object, "create");

            // Make event.create request.
            let context = TestContext::new(db, authz);

            let payload = CreateRequest {
                room_id: room.id(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "hello" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let err = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect_err("Unexpected success on event creation");

            assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
        });
    }

    #[test]
    fn create_event_missing_room() {
        futures::executor::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = CreateRequest {
                room_id: Uuid::new_v4(),
                kind: String::from("message"),
                set: Some(String::from("messages")),
                label: Some(String::from("message-1")),
                data: json!({ "text": "hello" }),
                is_claim: false,
                is_persistent: true,
                priority: DEFAULT_PRIORITY,
            };

            let err = handle_request::<CreateHandler>(&context, &agent, payload)
                .await
                .expect_err("Unexpected success on event creation");

            assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
        });
    }

    ///////////////////////////////////////////////////////////////////////////

    #[test]
    fn list_events() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let (room, db_events) = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                // Create room.
                let room = shared_helpers::insert_room(&conn);

                // Create events in the room.
                let events = (1..4)
                    .map(|i| {
                        factory::Event::new()
                            .room_id(room.id())
                            .kind("message")
                            .data(&json!({ "text": format!("message {}", i) }))
                            .occurred_at(i * 1000)
                            .created_by(&agent.agent_id())
                            .insert(&conn)
                    })
                    .collect::<Vec<Event>>();

                (room, events)
            };

            // Allow agent to list events in the room.
            let mut authz = TestAuthz::new();
            let room_id = room.id().to_string();
            let object = vec!["rooms", &room_id, "events"];
            authz.allow(agent.account_id(), object, "list");

            // Make event.list request.
            let context = TestContext::new(db, authz);

            let payload = ListRequest {
                room_id: room.id(),
                kind: None,
                set: None,
                label: None,
                last_occurred_at: None,
                direction: Direction::Backward,
                limit: Some(2),
            };

            let messages = handle_request::<ListHandler>(&context, &agent, payload)
                .await
                .expect("Events listing failed (page 1)");

            // Assert last two events response.
            let (events, respp) = find_response::<Vec<Event>>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].id(), db_events[2].id());
            assert_eq!(events[1].id(), db_events[1].id());

            // Request the next page.
            let payload = ListRequest {
                room_id: room.id(),
                kind: None,
                set: None,
                label: None,
                last_occurred_at: Some(events[1].occurred_at()),
                direction: Direction::Backward,
                limit: Some(2),
            };

            let messages = handle_request::<ListHandler>(&context, &agent, payload)
                .await
                .expect("Events listing failed (page 2)");

            // Assert the first event.
            let (events, respp) = find_response::<Vec<Event>>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::OK);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].id(), db_events[0].id());
        });
    }

    #[test]
    fn list_events_not_authorized() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let room = {
                let conn = db
                    .connection_pool()
                    .get()
                    .expect("Failed to get DB connection");

                shared_helpers::insert_room(&conn)
            };

            let context = TestContext::new(db, TestAuthz::new());

            let payload = ListRequest {
                room_id: room.id(),
                kind: None,
                set: None,
                label: None,
                last_occurred_at: None,
                direction: Direction::Backward,
                limit: Some(2),
            };

            let err = handle_request::<ListHandler>(&context, &agent, payload)
                .await
                .expect_err("Unexpected success on events listing");

            assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
        });
    }

    #[test]
    fn list_events_missing_room() {
        futures::executor::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let context = TestContext::new(TestDb::new(), TestAuthz::new());

            let payload = ListRequest {
                room_id: Uuid::new_v4(),
                kind: None,
                set: None,
                label: None,
                last_occurred_at: None,
                direction: Direction::Backward,
                limit: Some(2),
            };

            let err = handle_request::<ListHandler>(&context, &agent, payload)
                .await
                .expect_err("Unexpected success on events listing");

            assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
        });
    }
}
