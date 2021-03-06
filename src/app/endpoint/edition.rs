use anyhow::Context as AnyhowContext;
use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::FutureExt;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use svc_agent::{
    mqtt::{
        IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
        ResponseStatus, ShortTermTimingProperties,
    },
    Addressable,
};
use svc_authn::Authenticable;
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::operations::commit_edition;
use crate::db;
use crate::db::adjustment::Segments;

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct CreateHandler;

#[derive(Debug, Deserialize)]
pub(crate) struct CreateRequest {
    room_id: Uuid,
}

#[async_trait]
impl RequestHandler for CreateHandler {
    type Payload = CreateRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room = helpers::find_room(
            context,
            payload.room_id,
            helpers::RoomTimeRequirement::Any,
            reqp.method(),
        )
        .await?;

        let object = {
            let object = room.authz_object();
            let object = object.iter().map(|s| s.as_ref()).collect::<Vec<_>>();
            AuthzObject::new(&object).into()
        };

        let authz_time = context
            .authz()
            .authorize(
                room.audience().to_owned(),
                reqp.as_account_id().to_owned(),
                object,
                "update".into(),
            )
            .await?;

        let edition = {
            let query = db::edition::InsertQuery::new(payload.room_id, reqp.as_agent_id());
            let mut conn = context.get_conn().await?;

            context
                .profiler()
                .measure(
                    (
                        ProfilerKeys::EditionInsertQuery,
                        Some(reqp.method().to_owned()),
                    ),
                    query.execute(&mut conn),
                )
                .await
                .context("Failed to insert edition")
                .error(AppErrorKind::DbQueryFailed)?
        };

        context.add_logger_tags(o!("edition_id" => edition.id().to_string()));

        let response = helpers::build_response(
            ResponseStatus::CREATED,
            edition.clone(),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        let notification = helpers::build_notification(
            "edition.create",
            &format!("rooms/{}/editions", payload.room_id),
            edition,
            reqp,
            context.start_timestamp(),
        );

        Ok(Box::new(stream::from_iter(vec![response, notification])))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListHandler;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRequest {
    room_id: Uuid,
    last_created_at: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

#[async_trait]
impl RequestHandler for ListHandler {
    type Payload = ListRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room = helpers::find_room(
            context,
            payload.room_id,
            helpers::RoomTimeRequirement::Any,
            reqp.method(),
        )
        .await?;

        let object = AuthzObject::room(&room).into();

        let authz_time = context
            .authz()
            .authorize(
                room.audience().into(),
                reqp.as_account_id().to_owned(),
                object,
                "update".into(),
            )
            .await?;

        let mut query = db::edition::ListQuery::new(room.id());

        if let Some(last_created_at) = payload.last_created_at {
            query = query.last_created_at(last_created_at);
        }

        if let Some(limit) = payload.limit {
            query = query.limit(limit);
        }

        let editions = {
            let mut conn = context.get_ro_conn().await?;

            context
                .profiler()
                .measure(
                    (
                        ProfilerKeys::EditionListQuery,
                        Some(reqp.method().to_owned()),
                    ),
                    query.execute(&mut conn),
                )
                .await
                .context("Failed to list editions")
                .error(AppErrorKind::DbQueryFailed)?
        };

        // Respond with events list.
        Ok(Box::new(stream::once(helpers::build_response(
            ResponseStatus::OK,
            editions,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ))))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DeleteHandler;

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteRequest {
    id: Uuid,
}

#[async_trait]
impl RequestHandler for DeleteHandler {
    type Payload = DeleteRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let (edition, room) = {
            let query = db::edition::FindWithRoomQuery::new(payload.id);
            let mut conn = context.get_ro_conn().await?;

            let maybe_edition = context
                .profiler()
                .measure(
                    (
                        ProfilerKeys::EditionFindWithRoomQuery,
                        Some(reqp.method().to_owned()),
                    ),
                    query.execute(&mut conn),
                )
                .await
                .context("Failed to find edition with room")
                .error(AppErrorKind::DbQueryFailed)?;

            match maybe_edition {
                Some(edition_with_room) => edition_with_room,
                None => {
                    return Err(anyhow!("Edition not found")).error(AppErrorKind::EditionNotFound);
                }
            }
        };

        helpers::add_room_logger_tags(context, &room);
        context.add_logger_tags(o!("edition_id" => edition.id().to_string()));

        let object = AuthzObject::room(&room).into();

        let authz_time = context
            .authz()
            .authorize(
                room.audience().into(),
                reqp.as_account_id().to_owned(),
                object,
                "update".into(),
            )
            .await?;

        {
            let query = db::edition::DeleteQuery::new(edition.id());
            let mut conn = context.get_conn().await?;

            context
                .profiler()
                .measure(
                    (
                        ProfilerKeys::EditionDeleteQuery,
                        Some(reqp.method().to_owned()),
                    ),
                    query.execute(&mut conn),
                )
                .await
                .context("Failed to delete edition")
                .error(AppErrorKind::DbQueryFailed)?;
        }

        let response = helpers::build_response(
            ResponseStatus::OK,
            edition,
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        );

        Ok(Box::new(stream::from_iter(vec![response])))
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct CommitHandler;

#[derive(Debug, Deserialize)]
pub(crate) struct CommitRequest {
    id: Uuid,
}

#[async_trait]
impl RequestHandler for CommitHandler {
    type Payload = CommitRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        // Find edition with its source room.
        let (edition, room) = {
            let query = db::edition::FindWithRoomQuery::new(payload.id);
            let mut conn = context.get_ro_conn().await?;

            let maybe_edition = context
                .profiler()
                .measure(
                    (
                        ProfilerKeys::EditionFindWithRoomQuery,
                        Some(reqp.method().to_owned()),
                    ),
                    query.execute(&mut conn),
                )
                .await
                .context("Failed to find edition with room")
                .error(AppErrorKind::DbQueryFailed)?;

            match maybe_edition {
                Some(edition_with_room) => edition_with_room,
                None => {
                    return Err(anyhow!("Edition not found")).error(AppErrorKind::EditionNotFound);
                }
            }
        };

        helpers::add_room_logger_tags(context, &room);
        context.add_logger_tags(o!("edition_id" => edition.id().to_string()));

        // Authorize room update.
        let object = AuthzObject::room(&room).into();

        let authz_time = context
            .authz()
            .authorize(
                room.audience().into(),
                reqp.as_account_id().to_owned(),
                object,
                "update".into(),
            )
            .await?;

        // Run commit task asynchronously.
        let db = context.db().to_owned();
        let profiler = context.profiler();
        let logger = context.logger().new(o!());

        let notification_future = async_std::task::spawn(async move {
            let result = commit_edition(&db, &profiler, &edition, &room).await;

            // Handle result.
            let result = match result {
                Ok((destination, modified_segments)) => EditionCommitResult::Success {
                    source_room_id: edition.source_room_id(),
                    committed_room_id: destination.id(),
                    modified_segments,
                },
                Err(err) => {
                    error!(logger, "Room adjustment job failed: {}", err);
                    let app_error = AppError::new(AppErrorKind::EditionCommitTaskFailed, err);
                    app_error.notify_sentry(&logger);
                    EditionCommitResult::Error {
                        error: app_error.to_svc_error(),
                    }
                }
            };

            // Publish success/failure notification.
            let notification = EditionCommitNotification {
                status: result.status(),
                tags: room.tags().map(|t| t.to_owned()),
                result,
            };

            let timing = ShortTermTimingProperties::new(Utc::now());
            let props = OutgoingEventProperties::new("edition.commit", timing);
            let path = format!("audiences/{}/events", room.audience());
            let event = OutgoingEvent::broadcast(notification, props, &path);

            Box::new(event) as Box<dyn IntoPublishableMessage + Send>
        });

        // Respond with 202.
        // The actual task result will be broadcasted to the room topic when finished.
        let response = stream::once(helpers::build_response(
            ResponseStatus::ACCEPTED,
            json!({}),
            reqp,
            context.start_timestamp(),
            Some(authz_time),
        ));

        let notification = notification_future.into_stream();
        Ok(Box::new(response.chain(notification)))
    }
}

#[derive(Serialize)]
struct EditionCommitNotification {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<JsonValue>,
    #[serde(flatten)]
    result: EditionCommitResult,
}

#[derive(Serialize)]
#[serde(untagged)]
enum EditionCommitResult {
    Success {
        source_room_id: Uuid,
        committed_room_id: Uuid,
        #[serde(with = "crate::db::adjustment::serde::segments")]
        modified_segments: Segments,
    },
    Error {
        error: SvcError,
    },
}

impl EditionCommitResult {
    fn status(&self) -> &'static str {
        match self {
            Self::Success { .. } => "success",
            Self::Error { .. } => "error",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    mod create {
        use super::super::*;
        use crate::db::edition::Object as Edition;
        use crate::test_helpers::prelude::*;

        #[test]
        fn create_edition() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    let mut conn = db.get_conn().await;
                    shared_helpers::insert_room(&mut conn).await
                };

                // Allow agent to create editions
                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id];
                authz.allow(agent.account_id(), object, "update");

                // Make edition.create request
                let mut context = TestContext::new(db, authz);
                let payload = CreateRequest { room_id: room.id() };

                let messages = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Failed to create edition");

                // Assert response
                let (edition, respp, _) = find_response::<Edition>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::CREATED);
                assert_eq!(edition.source_room_id(), room.id());
            });
        }

        #[test]
        fn create_edition_not_authorized() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let room = {
                    let mut conn = db.get_conn().await;
                    shared_helpers::insert_room(&mut conn).await
                };

                let mut context = TestContext::new(db, TestAuthz::new());
                let payload = CreateRequest { room_id: room.id() };

                let response = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success creating edition with no authorization");

                assert_eq!(response.status(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn create_edition_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new().await, TestAuthz::new());

                let payload = CreateRequest {
                    room_id: Uuid::new_v4(),
                };

                let err = handle_request::<CreateHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success creating edition for no room");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod list {
        use super::super::*;
        use crate::db::edition::Object as Edition;
        use crate::test_helpers::prelude::*;

        #[test]
        fn list_editions() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let (room, editions) = {
                    let mut conn = db.get_conn().await;
                    let room = shared_helpers::insert_room(&mut conn).await;

                    let edition = factory::Edition::new(room.id(), agent.agent_id())
                        .insert(&mut conn)
                        .await;

                    (room, vec![edition])
                };

                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id];
                authz.allow(agent.account_id(), object, "update");

                let mut context = TestContext::new(db, authz);

                let payload = ListRequest {
                    room_id: room.id(),
                    last_created_at: None,
                    limit: None,
                };

                let messages = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Failed to list editions");

                let (resp_editions, respp, _) = find_response::<Vec<Edition>>(messages.as_slice());
                assert_eq!(respp.status(), ResponseStatus::OK);
                assert_eq!(resp_editions.len(), editions.len());
                assert_eq!(resp_editions[0].id(), editions[0].id());
            });
        }

        #[test]
        fn list_editions_not_authorized() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let (room, _editions) = {
                    let mut conn = db.get_conn().await;
                    let room = shared_helpers::insert_room(&mut conn).await;

                    let edition = factory::Edition::new(room.id(), agent.agent_id())
                        .insert(&mut conn)
                        .await;

                    (room, vec![edition])
                };

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = ListRequest {
                    room_id: room.id(),
                    last_created_at: None,
                    limit: None,
                };

                let resp = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success without authorization on editions list");

                assert_eq!(resp.status(), ResponseStatus::FORBIDDEN);
            });
        }

        #[test]
        fn list_editions_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new().await, TestAuthz::new());

                let payload = ListRequest {
                    room_id: Uuid::new_v4(),
                    last_created_at: None,
                    limit: None,
                };

                let err = handle_request::<ListHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success listing editions for no room");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "room_not_found");
            });
        }
    }

    mod delete {
        use super::super::*;
        use crate::db::edition::Object as Edition;
        use crate::test_helpers::prelude::*;

        #[test]
        fn delete_edition() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let (room, editions) = {
                    let mut conn = db.get_conn().await;
                    let room = shared_helpers::insert_room(&mut conn).await;
                    let mut editions = vec![];

                    for _ in 1..4 {
                        let edition = factory::Edition::new(room.id(), agent.agent_id())
                            .insert(&mut conn)
                            .await;

                        editions.push(edition);
                    }

                    (room, editions)
                };

                let mut authz = TestAuthz::new();
                let room_id = room.id().to_string();
                let object = vec!["rooms", &room_id];
                authz.allow(agent.account_id(), object, "update");

                let mut context = TestContext::new(db, authz);

                let payload = DeleteRequest {
                    id: editions[0].id(),
                };

                let messages = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect("Failed to find deleted edition");

                let (resp_edition, resp, _) = find_response::<Edition>(messages.as_slice());
                assert_eq!(resp.status(), ResponseStatus::OK);
                assert_eq!(resp_edition.id(), editions[0].id());

                let mut conn = context
                    .db()
                    .acquire()
                    .await
                    .expect("Failed to get DB connection");

                let db_editions = db::edition::ListQuery::new(room.id())
                    .execute(&mut conn)
                    .await
                    .expect("Failed to fetch editions");

                assert_eq!(db_editions.len(), editions.len() - 1);
            });
        }

        #[test]
        fn delete_edition_not_authorized() {
            async_std::task::block_on(async {
                let db = TestDb::new().await;
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

                let (room, editions) = {
                    let mut conn = db.get_conn().await;
                    let room = shared_helpers::insert_room(&mut conn).await;
                    let mut editions = vec![];

                    for _ in 1..4 {
                        let edition = factory::Edition::new(room.id(), agent.agent_id())
                            .insert(&mut conn)
                            .await;

                        editions.push(edition)
                    }

                    (room, editions)
                };

                let mut context = TestContext::new(db, TestAuthz::new());

                let payload = DeleteRequest {
                    id: editions[0].id(),
                };

                let resp = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success without authorization on editions list");

                assert_eq!(resp.status(), ResponseStatus::FORBIDDEN);

                let mut conn = context
                    .db()
                    .acquire()
                    .await
                    .expect("Failed to get DB connection");

                let db_editions = db::edition::ListQuery::new(room.id())
                    .execute(&mut conn)
                    .await
                    .expect("Failed to fetch editions");

                assert_eq!(db_editions.len(), editions.len());
            });
        }

        #[test]
        fn delete_editions_missing_room() {
            async_std::task::block_on(async {
                let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
                let mut context = TestContext::new(TestDb::new().await, TestAuthz::new());
                let payload = DeleteRequest { id: Uuid::new_v4() };

                let err = handle_request::<DeleteHandler>(&mut context, &agent, payload)
                    .await
                    .expect_err("Unexpected success listing editions for no room");

                assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
                assert_eq!(err.kind(), "edition_not_found");
            });
        }
    }
}
