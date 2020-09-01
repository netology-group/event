use std::ops::Bound;

use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{error, warn};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        OutgoingRequest, ResponseStatus, ShortTermTimingProperties,
    },
    Addressable, AgentId,
};
use svc_error::{extension::sentry, Error as SvcError};
use tracing::warn as twarn;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::{metric::ProfilerKeys, prelude::*};
use crate::app::operations::adjust_room;
use crate::db::adjustment::Segment;
use crate::db::agent;
use crate::db::room::{now, since_now, FindQuery, InsertQuery, Time, UpdateQuery};

///////////////////////////////////////////////////////////////////////////////

const MQTT_GW_API_VERSION: &str = "v1";

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

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    audience: String,
    #[serde(with = "crate::serde::ts_seconds_bound_tuple")]
    time: Time,
    tags: Option<JsonValue>,
}

pub(crate) struct CreateHandler;

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;
    const ERROR_TITLE: &'static str = "Failed to create room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        // Validate opening time.
        match payload.time {
            (Bound::Included(opened_at), Bound::Excluded(closed_at)) if closed_at > opened_at => (),
            _ => return Err("Invalid room time").status(ResponseStatus::BAD_REQUEST),
        }

        // Authorize room creation on the tenant.
        let authz_time = context
            .authz()
            .authorize(&payload.audience, reqp, vec!["rooms"], "create")
            .await?;

        // Insert room.
        let room = {
            let mut query = InsertQuery::new(&payload.audience, payload.time);

            if let Some(tags) = payload.tags {
                query = query.tags(tags);
            }

            let conn = context.db().get()?;

            context
                .profiler()
                .measure(ProfilerKeys::RoomInsertQuery, || query.execute(&conn))?
        };

        // Respond and broadcast to the audience topic.
        let response = helpers::build_response(
            ResponseStatus::CREATED,
            room.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.create",
            &format!("audiences/{}/events", payload.audience),
            room,
            reqp,
            start_timestamp,
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct ReadRequest {
    id: Uuid,
}

pub(crate) struct ReadHandler;

#[async_trait]
impl RequestHandler for ReadHandler {
    type Payload = ReadRequest;
    const ERROR_TITLE: &'static str = "Failed to read room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let query = FindQuery::new(payload.id);
            let conn = context.ro_db().get()?;

            context
                .profiler()
                .measure(ProfilerKeys::RoomFindQuery, || query.execute(&conn))?
                .ok_or_else(|| format!("Room not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room reading on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "read")
            .await?;

        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            room,
            reqp,
            start_timestamp,
            Some(authz_time),
        ))))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateRequest {
    id: Uuid,
    #[serde(default)]
    #[serde(with = "crate::serde::ts_seconds_option_bound_tuple")]
    time: Option<Time>,
    tags: Option<JsonValue>,
}

pub(crate) struct UpdateHandler;

#[async_trait]
impl RequestHandler for UpdateHandler {
    type Payload = UpdateRequest;
    const ERROR_TITLE: &'static str = "Failed to update room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        // Find not closed room.
        let mut query = FindQuery::new(payload.id);

        if payload.time.is_some() {
            query = query.time(since_now());
        }

        let room = {
            let conn = context.ro_db().get()?;

            context
                .profiler()
                .measure(ProfilerKeys::RoomFindQuery, || query.execute(&conn))?
                .ok_or_else(|| format!("Room not found, id = '{}' or closed", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize room reading on the tenant.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "update")
            .await?;

        // Validate opening time.
        let mut time = payload.time;
        if let Some(new_time) = time {
            match new_time {
                (Bound::Included(new_opened_at), Bound::Excluded(new_closed_at))
                    if new_closed_at > new_opened_at =>
                {
                    if let (Bound::Included(opened_at), _) = room.time() {
                        if *opened_at <= Utc::now() {
                            let new_closed_at = if new_closed_at < Utc::now() {
                                Utc::now()
                            } else {
                                new_closed_at
                            };

                            time =
                                Some((Bound::Included(*opened_at), Bound::Excluded(new_closed_at)));
                        }
                    }
                }
                _ => return Err("Invalid room time").status(ResponseStatus::BAD_REQUEST),
            }
        }

        // Update room.
        let room = {
            let mut query = UpdateQuery::new(room.id());

            if let Some(time) = time {
                query = query.time(time);
            }

            if let Some(tags) = payload.tags {
                query = query.tags(tags);
            }

            let conn = context.db().get()?;

            context
                .profiler()
                .measure(ProfilerKeys::RoomUpdateQuery, || query.execute(&conn))?
        };

        // Respond and broadcast to the audience topic.
        let response = helpers::build_response(
            ResponseStatus::OK,
            room.clone(),
            reqp,
            start_timestamp,
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "room.update",
            &format!("audiences/{}/events", room.audience()),
            room,
            reqp,
            start_timestamp,
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct EnterRequest {
    id: Uuid,
}

pub(crate) struct EnterHandler;

#[async_trait]
impl RequestHandler for EnterHandler {
    type Payload = EnterRequest;
    const ERROR_TITLE: &'static str = "Failed to enter room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        twarn!("Finding room");

        let room = {
            let query = FindQuery::new(payload.id).time(now());
            twarn!("Acquiring db connection");
            let conn = context.ro_db().get()?;

            twarn!("Db request");
            context
                .profiler()
                .measure(ProfilerKeys::RoomFindQuery, || query.execute(&conn))?
                .ok_or_else(|| format!("Room not found or closed, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };
        twarn!("Room found");

        // Authorize subscribing to the room's events.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id, "events"];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object.clone(), "subscribe")
            .await?;
        twarn!("Room access authorized, building agent");

        // Register agent in `in_progress` state.
        {
            let conn = context.db().get()?;
            let query = agent::InsertQuery::new(reqp.as_agent_id(), room.id());
            twarn!("Agent db request");

            context
                .profiler()
                .measure(ProfilerKeys::AgentInsertQuery, || query.execute(&conn))?;
        }
        twarn!("Agent inserted");

        // Send dynamic subscription creation request to the broker.
        let payload = SubscriptionRequest::new(reqp.as_agent_id().to_owned(), object);

        let mut short_term_timing = ShortTermTimingProperties::until_now(start_timestamp);
        short_term_timing.set_authorization_time(authz_time);

        let props = reqp.to_request(
            "subscription.create",
            reqp.response_topic(),
            reqp.correlation_data(),
            short_term_timing,
        );

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

        twarn!("Handler done");

        Ok(Box::new(stream::once(boxed_request)))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct LeaveRequest {
    id: Uuid,
}

pub(crate) struct LeaveHandler;

#[async_trait]
impl RequestHandler for LeaveHandler {
    type Payload = LeaveRequest;
    const ERROR_TITLE: &'static str = "Failed to leave room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let (room, presence) = {
            let query = FindQuery::new(payload.id);
            let conn = context.ro_db().get()?;

            let room = context
                .profiler()
                .measure(ProfilerKeys::RoomFindQuery, || query.execute(&conn))?
                .ok_or_else(|| format!("Room not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?;

            // Check room presence.
            let query = agent::ListQuery::new()
                .room_id(room.id())
                .agent_id(reqp.as_agent_id())
                .status(agent::Status::Ready);

            let presence = context
                .profiler()
                .measure(ProfilerKeys::AgentListQuery, || query.execute(&conn))?;

            (room, presence)
        };

        if presence.is_empty() {
            return Err(format!(
                "agent = '{}' is not online in the room = '{}'",
                reqp.as_agent_id(),
                room.id()
            ))
            .status(ResponseStatus::NOT_FOUND);
        }

        // Send dynamic subscription deletion request to the broker.
        let room_id = room.id().to_string();

        let payload = SubscriptionRequest::new(
            reqp.as_agent_id().to_owned(),
            vec!["rooms", &room_id, "events"],
        );

        let props = reqp.to_request(
            "subscription.delete",
            reqp.response_topic(),
            reqp.correlation_data(),
            ShortTermTimingProperties::until_now(start_timestamp),
        );

        // FIXME: It looks like sending a request to the client but the broker intercepts it
        //        deletes the subscription and replaces the request with the response.
        //        This is kind of ugly but it guaranties that the request will be processed by
        //        the broker node where the client is connected to. We need that because
        //        the request changes local state on that node.
        //        A better solution will be possible after resolution of this issue:
        //        https://github.com/vernemq/vernemq/issues/1326.
        //        Then we won't need the local state on the broker at all and will be able
        //        to send a multicast request to the broker.
        let outgoing_request = OutgoingRequest::unicast(payload, props, reqp, MQTT_GW_API_VERSION);
        let boxed_request = Box::new(outgoing_request) as Box<dyn IntoPublishableMessage + Send>;
        Ok(Box::new(stream::once(boxed_request)))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
pub(crate) struct AdjustRequest {
    id: Uuid,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    started_at: DateTime<Utc>,
    #[serde(with = "crate::serde::milliseconds_bound_tuples")]
    segments: Vec<Segment>,
    offset: i64,
}

pub(crate) struct AdjustHandler;

#[async_trait]
impl RequestHandler for AdjustHandler {
    type Payload = AdjustRequest;
    const ERROR_TITLE: &'static str = "Failed to adjust room";

    async fn handle<C: Context>(
        context: &C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
        start_timestamp: DateTime<Utc>,
    ) -> Result {
        let room = {
            let query = FindQuery::new(payload.id);
            let conn = context.ro_db().get()?;

            context
                .profiler()
                .measure(ProfilerKeys::RoomFindQuery, || query.execute(&conn))?
                .ok_or_else(|| format!("Room not found, id = '{}'", payload.id))
                .status(ResponseStatus::NOT_FOUND)?
        };

        // Authorize trusted account for the room's audience.
        let room_id = room.id().to_string();
        let object = vec!["rooms", &room_id];

        let authz_time = context
            .authz()
            .authorize(room.audience(), reqp, object, "update")
            .await?;

        // Run asynchronous task for adjustment.
        let db = context.db().to_owned();

        // Respond with 202.
        // The actual task result will be broadcasted to the room topic when finished.
        let response = stream::once(helpers::build_response(
            ResponseStatus::ACCEPTED,
            json!({}),
            reqp,
            start_timestamp,
            Some(authz_time),
        ));

        let mut task_finished = false;

        let notification = stream::from_fn(move || {
            if task_finished {
                return None;
            }

            // Call room adjustment operation.
            let operation_result = adjust_room(
                &db,
                &room,
                payload.started_at,
                &payload.segments,
                payload.offset,
            );

            // Handle result.
            let result = match operation_result {
                Ok((original_room, modified_room, modified_segments)) => {
                    RoomAdjustResult::Success {
                        original_room_id: original_room.id(),
                        modified_room_id: modified_room.id(),
                        modified_segments,
                    }
                }
                Err(err) => {
                    error!(
                        "Room adjustment job failed for room_id = '{}': {}",
                        room.id(),
                        err
                    );

                    let error = SvcError::builder()
                        .status(ResponseStatus::UNPROCESSABLE_ENTITY)
                        .kind("room.adjust", AdjustHandler::ERROR_TITLE)
                        .detail(&err.to_string())
                        .build();

                    sentry::send(error.clone())
                        .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));

                    RoomAdjustResult::Error { error }
                }
            };

            // Publish success/failure notification.
            let notification = RoomAdjustNotification {
                status: result.status(),
                tags: room.tags().map(|t| t.to_owned()),
                result,
            };

            let timing = ShortTermTimingProperties::new(Utc::now());
            let props = OutgoingEventProperties::new("room.adjust", timing);
            let path = format!("audiences/{}/events", room.audience());
            let event = OutgoingEvent::broadcast(notification, props, &path);

            task_finished = true;
            Some(Box::new(event) as Box<dyn IntoPublishableMessage + Send>)
        });

        Ok(Box::new(response.chain(notification)))
    }
}

#[derive(Serialize)]
struct RoomAdjustNotification {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<JsonValue>,
    #[serde(flatten)]
    result: RoomAdjustResult,
}

#[derive(Serialize)]
#[serde(untagged)]
enum RoomAdjustResult {
    Success {
        original_room_id: Uuid,
        modified_room_id: Uuid,
        #[serde(with = "crate::serde::milliseconds_bound_tuples")]
        modified_segments: Vec<Segment>,
    },
    Error {
        error: SvcError,
    },
}

impl RoomAdjustResult {
    fn status(&self) -> &'static str {
        match self {
            Self::Success { .. } => "success",
            Self::Error { .. } => "error",
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use serde_derive::Deserialize;

    use super::AgentId;

    #[derive(Deserialize)]
    struct DynSubRequest {
        subject: AgentId,
        object: Vec<String>,
    }

    mod create {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};
        use serde_json::json;

        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn create_room() {
            futures::executor::block_on(async {
                // Allow agent to create rooms.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                authz.allow(agent.account_id(), vec!["rooms"], "create");

                // Make room.create request.
                let context = TestContext::new(TestDb::new(), authz);
                let now = Utc::now().trunc_subsecs(0);

                let time = (
                    Bound::Included(now + Duration::hours(1)),
                    Bound::Excluded(now + Duration::hours(2)),
                );

                let tags = json!({ "webinar_id": "123" });

                let payload = CreateRequest {
                    time: time.clone(),
                    audience: USR_AUDIENCE.to_owned(),
                    tags: Some(tags.clone()),
                };

                let messages = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room creation failed");

                // Assert response.
                let (room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.tags(), Some(&tags));

                // Assert notification.
                let (room, evp, topic) = find_event::<Room>(messages.as_slice());
                assert!(topic.ends_with(&format!("/audiences/{}/events", USR_AUDIENCE)));
                assert_eq!(evp.label(), "room.create");
                assert_eq!(room.audience(), USR_AUDIENCE);
                assert_eq!(room.time(), &time);
                assert_eq!(room.tags(), Some(&tags));
            });
        }

        #[test]
        fn create_room_not_authorized() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                // Make room.create request.
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let now = Utc::now().trunc_subsecs(0);

                let time = (
                    Bound::Included(now + Duration::hours(1)),
                    Bound::Excluded(now + Duration::hours(2)),
                );

                let payload = CreateRequest {
                    time: time.clone(),
                    audience: USR_AUDIENCE.to_owned(),
                    tags: None,
                };

                let err = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room creation");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn create_room_invalid_time() {
            futures::executor::block_on(async {
                // Allow agent to create rooms.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                authz.allow(agent.account_id(), vec!["rooms"], "create");

                // Make room.create request.
                let context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = CreateRequest {
                    time: (Bound::Unbounded, Bound::Unbounded),
                    audience: USR_AUDIENCE.to_owned(),
                    tags: None,
                };

                let err = handle_request::<CreateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room creation");

                assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST);
            });
        }
    }

    mod read {
        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn read_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Allow agent to read the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "read");

                // Make room.read request.
                let context = TestContext::new(db, authz);
                let payload = ReadRequest { id: room.id() };

                let messages = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect("Room reading failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), room.time());
                assert_eq!(resp_room.tags(), room.tags());
            });
        }

        #[test]
        fn read_room_not_authorized() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Make room.read request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = ReadRequest { id: room.id() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room reading");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn read_room_missing() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = ReadRequest { id: Uuid::new_v4() };

                let err = handle_request::<ReadHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room reading");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod update {
        use std::ops::Bound;

        use chrono::{Duration, SubsecRound, Utc};

        use crate::db::room::Object as Room;
        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn update_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();
                let now = Utc::now().trunc_subsecs(0);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now + Duration::hours(1)),
                            Bound::Excluded(now + Duration::hours(2)),
                        ))
                        .tags(&json!({ "webinar_id": "123" }))
                        .insert(&conn)
                };

                // Allow agent to update the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

                // Make room.update request.
                let context = TestContext::new(db, authz);

                let time = (
                    Bound::Included(now + Duration::hours(2)),
                    Bound::Excluded(now + Duration::hours(3)),
                );

                let tags = json!({"webinar_id": "456789"});

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some(time),
                    tags: Some(tags.clone()),
                };

                let messages = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room update failed");

                // Assert response.
                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.id(), room.id());
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(resp_room.time(), &time);
                assert_eq!(resp_room.tags(), Some(&tags));
            });
        }

        #[test]
        fn update_closed_at_in_open_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();
                let now = Utc::now().trunc_subsecs(0);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now - Duration::hours(1)),
                            Bound::Excluded(now + Duration::hours(1)),
                        ))
                        .insert(&conn)
                };

                // Allow agent to update the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

                // Make room.update request.
                let context = TestContext::new(db, authz);

                let time = (
                    Bound::Included(now + Duration::hours(1)),
                    Bound::Excluded(now + Duration::hours(3)),
                );

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some(time),
                    tags: None,
                };

                let messages = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room update failed");

                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.id(), room.id());
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(
                    resp_room.time(),
                    &(
                        Bound::Included(now - Duration::hours(1)),
                        Bound::Excluded(now + Duration::hours(3)),
                    )
                );
            });
        }

        #[test]
        fn update_closed_at_in_the_past_in_already_open_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();
                let now = Utc::now().trunc_subsecs(0);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now - Duration::hours(2)),
                            Bound::Excluded(now + Duration::hours(2)),
                        ))
                        .insert(&conn)
                };

                // Allow agent to update the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

                // Make room.update request.
                let context = TestContext::new(db, authz);

                let time = (
                    Bound::Included(now - Duration::hours(2)),
                    Bound::Excluded(now - Duration::hours(1)),
                );

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some(time),
                    tags: None,
                };

                let messages = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect("Room update failed");

                let (resp_room, respp) = find_response::<Room>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_room.id(), room.id());
                assert_eq!(resp_room.audience(), room.audience());
                assert_eq!(
                    resp_room.time(),
                    &(
                        Bound::Included(now - Duration::hours(2)),
                        Bound::Excluded(now),
                    )
                );
            });
        }

        #[test]
        fn update_room_invalid_time() {
            futures::executor::block_on(async {
                let db = TestDb::new();
                let now = Utc::now().trunc_subsecs(0);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    factory::Room::new()
                        .audience(USR_AUDIENCE)
                        .time((
                            Bound::Included(now + Duration::hours(1)),
                            Bound::Excluded(now + Duration::hours(2)),
                        ))
                        .insert(&conn)
                };

                // Allow agent to update the room.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                authz.allow(agent.account_id(), vec!["rooms", &room_id], "update");

                // Make room.update request.
                let context = TestContext::new(db, authz);

                let time = (
                    Bound::Included(now + Duration::hours(1)),
                    Bound::Excluded(now - Duration::hours(2)),
                );

                let payload = UpdateRequest {
                    id: room.id(),
                    time: Some(time),
                    tags: None,
                };

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::BAD_REQUEST);
            });
        }

        #[test]
        fn update_room_not_authorized() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Make room.update request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = UpdateRequest {
                    id: room.id(),
                    time: None,
                    tags: None,
                };

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn update_room_missing() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = UpdateRequest {
                    id: Uuid::new_v4(),
                    time: None,
                    tags: None,
                };

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn update_room_closed() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create closed room.
                    shared_helpers::insert_closed_room(&conn)
                };

                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = UpdateRequest {
                    id: room.id(),
                    time: None,
                    tags: None,
                };

                let err = handle_request::<UpdateHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room update");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod enter {
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::DynSubRequest;

        #[test]
        fn enter_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Allow agent to subscribe to the rooms' events.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let context = TestContext::new(db, authz);
                let payload = EnterRequest { id: room.id() };

                let messages = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect("Room entrance failed");

                // Assert dynamic subscription request.
                let (payload, reqp, topic) = find_request::<DynSubRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    agent.agent_id(),
                    MQTT_GW_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(reqp.method(), "subscription.create");
                assert_eq!(payload.subject, agent.agent_id().to_owned());
                assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
            });
        }

        #[test]
        fn enter_room_not_authorized() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Make room.enter request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn enter_room_missing() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = EnterRequest { id: Uuid::new_v4() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn enter_room_closed() {
            futures::executor::block_on(async {
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create closed room.
                    shared_helpers::insert_closed_room(&conn)
                };

                // Allow agent to subscribe to the rooms' events.
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();

                authz.allow(
                    agent.account_id(),
                    vec!["rooms", &room_id, "events"],
                    "subscribe",
                );

                // Make room.enter request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = EnterRequest { id: room.id() };

                let err = handle_request::<EnterHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room entering");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod leave {
        use crate::test_helpers::prelude::*;

        use super::super::*;
        use super::DynSubRequest;

        #[test]
        fn leave_room() {
            futures::executor::block_on(async {
                let db = TestDb::new();
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    let room = shared_helpers::insert_room(&conn);

                    // Put agent online in the room.
                    shared_helpers::insert_agent(&conn, agent.agent_id(), room.id());
                    room
                };

                // Make room.leave request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = LeaveRequest { id: room.id() };

                let messages = handle_request::<LeaveHandler>(&context, &agent, payload)
                    .await
                    .expect("Room leaving failed");

                // Assert dynamic subscription request.
                let (payload, reqp, topic) = find_request::<DynSubRequest>(messages.as_slice());

                let expected_topic = format!(
                    "agents/{}/api/{}/in/{}",
                    agent.agent_id(),
                    MQTT_GW_API_VERSION,
                    context.config().id,
                );

                assert_eq!(topic, expected_topic);
                assert_eq!(reqp.method(), "subscription.delete");
                assert_eq!(&payload.subject, agent.agent_id());

                let room_id = room.id().to_string();
                assert_eq!(payload.object, vec!["rooms", &room_id, "events"]);
            });
        }

        #[test]
        fn leave_room_while_not_entered() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Make room.leave request.
                let context = TestContext::new(db, TestAuthz::new());
                let payload = LeaveRequest { id: room.id() };

                let err = handle_request::<LeaveHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room leaving");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }

        #[test]
        fn leave_room_missing() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());
                let payload = LeaveRequest { id: Uuid::new_v4() };

                let err = handle_request::<LeaveHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room leaving");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }

    mod adjust {
        use chrono::Utc;

        use crate::test_helpers::prelude::*;

        use super::super::*;

        #[test]
        fn adjust_room_not_authorized() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let db = TestDb::new();

                let room = {
                    let conn = db
                        .connection_pool()
                        .get()
                        .expect("Failed to get DB connection");

                    // Create room.
                    shared_helpers::insert_room(&conn)
                };

                // Make room.adjust request.
                let context = TestContext::new(db, TestAuthz::new());

                let payload = AdjustRequest {
                    id: room.id(),
                    started_at: Utc::now(),
                    segments: vec![],
                    offset: 0,
                };

                let err = handle_request::<AdjustHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room adjustment");

                assert_eq!(err.status_code(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn adjust_room_missing() {
            futures::executor::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let context = TestContext::new(TestDb::new(), TestAuthz::new());

                let payload = AdjustRequest {
                    id: Uuid::new_v4(),
                    started_at: Utc::now(),
                    segments: vec![],
                    offset: 0,
                };

                let err = handle_request::<AdjustHandler>(&context, &agent, payload)
                    .await
                    .expect_err("Unexpected success on room adjustment");

                assert_eq!(err.status_code(), ResponseStatus::NOT_FOUND);
            });
        }
    }
}
