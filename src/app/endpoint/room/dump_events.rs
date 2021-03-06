use async_std::prelude::*;
use async_std::stream;
use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableMessage, OutgoingEvent, OutgoingEventProperties,
    ResponseStatus, ShortTermTimingProperties,
};
use svc_error::Error as SvcError;
use uuid::Uuid;

use crate::app::context::Context;
use crate::app::endpoint::prelude::*;
use crate::app::operations::dump_events_to_s3;

#[derive(Debug, Deserialize)]
pub(crate) struct EventsDumpRequest {
    id: Uuid,
}

#[derive(Serialize)]
struct EventsDumpNotification {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags: Option<JsonValue>,
    result: EventsDumpResult,
}

#[derive(Serialize)]
#[serde(untagged)]
enum EventsDumpResult {
    Success { room_id: Uuid, s3_uri: String },
    Error { error: SvcError },
}

impl EventsDumpResult {
    fn status(&self) -> &'static str {
        match self {
            Self::Success { .. } => "success",
            Self::Error { .. } => "error",
        }
    }
}

pub(crate) struct EventsDumpHandler;

#[async_trait]
impl RequestHandler for EventsDumpHandler {
    type Payload = EventsDumpRequest;

    async fn handle<C: Context>(
        context: &mut C,
        payload: Self::Payload,
        reqp: &IncomingRequestProperties,
    ) -> Result {
        let room = helpers::find_room(
            context,
            payload.id,
            helpers::RoomTimeRequirement::Any,
            reqp.method(),
        )
        .await?;

        let object = AuthzObject::new(&["rooms"]).into();

        // Authorize room.
        let authz_time = context
            .authz()
            .authorize(
                room.audience().to_owned(),
                reqp.as_account_id().to_owned(),
                object,
                "dump_events".into(),
            )
            .await?;

        let db = context.db().to_owned();
        let profiler = context.profiler();
        let logger = context.logger().new(o!());

        let s3_client = context
            .s3_client()
            .ok_or_else(|| {
                error!(logger, "DumpEvents called with no s3client in context");
                anyhow!("No S3Client")
            })
            .error(AppErrorKind::NoS3Client)?;

        let notification_future = async_std::task::spawn(async move {
            let result = dump_events_to_s3(&db, &profiler, s3_client, &room).await;

            // Handle result.
            let result = match result {
                Ok(s3_uri) => EventsDumpResult::Success {
                    room_id: room.id(),
                    s3_uri,
                },
                Err(err) => {
                    error!(logger, "Events dump job failed: {}", err);
                    let app_error = AppError::new(AppErrorKind::EditionCommitTaskFailed, err);
                    app_error.notify_sentry(&logger);
                    EventsDumpResult::Error {
                        error: app_error.to_svc_error(),
                    }
                }
            };

            // Publish success/failure notification.
            let notification = EventsDumpNotification {
                status: result.status(),
                tags: room.tags().map(|t| t.to_owned()),
                result,
            };

            let timing = ShortTermTimingProperties::new(Utc::now());
            let props = OutgoingEventProperties::new("room.dump_events", timing);
            let path = format!("audiences/{}/events", room.audience());
            let event = OutgoingEvent::broadcast(notification, props, &path);

            Box::new(event) as Box<dyn IntoPublishableMessage + Send>
        });

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::prelude::*;

    #[test]
    fn dump_events_not_authorized() {
        async_std::task::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let db = TestDb::new().await;

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(db, TestAuthz::new());

            let payload = EventsDumpRequest { id: room.id() };

            let err = handle_request::<EventsDumpHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room dump");

            assert_eq!(err.status(), ResponseStatus::FORBIDDEN);
        });
    }

    #[test]
    fn dump_events_room_missing() {
        async_std::task::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let mut context = TestContext::new(TestDb::new().await, TestAuthz::new());

            let payload = EventsDumpRequest { id: Uuid::new_v4() };

            let err = handle_request::<EventsDumpHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room dump");

            assert_eq!(err.status(), ResponseStatus::NOT_FOUND);
            assert_eq!(err.kind(), "room_not_found");
        });
    }

    #[test]
    fn dump_events_no_s3_client() {
        async_std::task::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let db = TestDb::new().await;
            let mut authz = TestAuthz::new();
            authz.allow(agent.account_id(), vec!["rooms"], "dump_events");

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(TestDb::new().await, authz);

            let payload = EventsDumpRequest { id: room.id() };

            let err = handle_request::<EventsDumpHandler>(&mut context, &agent, payload)
                .await
                .expect_err("Unexpected success on room dump");

            assert_eq!(err.status(), ResponseStatus::NOT_IMPLEMENTED);
            assert_eq!(err.kind(), "no_s3_client");
        });
    }

    #[test]
    fn dump_events() {
        async_std::task::block_on(async {
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);
            let db = TestDb::new().await;
            let mut authz = TestAuthz::new();
            authz.allow(agent.account_id(), vec!["rooms"], "dump_events");

            let room = {
                let mut conn = db.get_conn().await;
                shared_helpers::insert_room(&mut conn).await
            };

            let mut context = TestContext::new(TestDb::new().await, authz);
            context.set_s3(shared_helpers::mock_s3());

            let payload = EventsDumpRequest { id: room.id() };

            let messages = handle_request::<EventsDumpHandler>(&mut context, &agent, payload)
                .await
                .expect("Failed to dump room events");

            assert_eq!(messages.len(), 2);
            let (_, respp, _) = find_response::<JsonValue>(messages.as_slice());
            let (ev, evp, _) = find_event::<JsonValue>(messages.as_slice());
            assert_eq!(respp.status(), ResponseStatus::ACCEPTED);
            assert_eq!(evp.label(), "room.dump_events");
            assert_eq!(
                ev.get("result")
                    .and_then(|v| v.get("room_id"))
                    .and_then(|v| v.as_str()),
                Some(room.id().to_string()).as_deref()
            );
            assert_eq!(
                ev.get("result")
                    .and_then(|v| v.get("s3_uri"))
                    .and_then(|v| v.as_str()),
                Some(format!(
                    "s3://eventsdump.{}/{}.json",
                    room.audience(),
                    room.id()
                ))
                .as_deref()
            );
        });
    }
}
