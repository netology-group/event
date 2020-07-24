use anyhow::{Context, Result};
use chrono::Utc;
use diesel::pg::PgConnection;
use log::info;
use svc_agent::AccountId;
use svc_authn::Authenticable;
use uuid::Uuid;

use crate::db::chat_notification::Object as ChatNotification;
use crate::db::event::Object as Event;
use crate::db::ConnectionPool as Db;

const DEFAULT_PRIORITY: i64 = 20;

pub(crate) fn call(db: &Db, event: &Event) -> Result<Vec<ChatNotification>> {
    info!(
        "Increment chat notifications task, event_id = '{}', room = {}, priority = {:?}",
        event.id(),
        event.room_id(),
        event.data().get("priority"),
    );

    let start_timestamp = Utc::now();
    let conn = db.get()?;

    let priority = event
        .data()
        .get("priority")
        .and_then(|s| s.as_i64())
        .unwrap_or(DEFAULT_PRIORITY);

    let result = increase_counters(
        &conn,
        event.room_id(),
        event.created_by().as_account_id(),
        priority as i32,
    );

    info!(
        "Increment chat notifications task successfully finished for event_id = '{}', room = {}, priority = {:?}, duration = {} ms",
        event.id(),
        event.room_id(),
        event.data().get("priority"),
        (Utc::now() - start_timestamp).num_milliseconds()
    );

    result
}

fn increase_counters(
    conn: &PgConnection,
    room_id: Uuid,
    author: &AccountId,
    priority: i32,
) -> Result<Vec<ChatNotification>> {
    use diesel::prelude::*;
    use diesel::sql_types::{Int4, Uuid};

    use crate::db::sql::Account_id;

    diesel::sql_query(INCREASE_COUNTERS_SQL)
        .bind::<Uuid, _>(room_id)
        .bind::<Account_id, _>(author)
        .bind::<Int4, _>(priority)
        .get_results(conn)
        .with_context(|| format!("Failed increasing counters",))
}

const INCREASE_COUNTERS_SQL: &str = r#"
UPDATE chat_notification
SET
    value = q.value + 1
FROM
    (SELECT id, value FROM chat_notification
        WHERE room_id = $1 AND account_id <> $2 AND priority = $3
        ORDER BY id
        FOR UPDATE
    ) AS q
WHERE q.id = chat_notification.id
RETURNING *
"#;

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::db::chat_notification::FindQuery;
    use crate::test_helpers::db::TestDb;
    use crate::test_helpers::prelude::*;

    #[test]
    fn increments_counter_with_proper_priority() {
        futures::executor::block_on(async {
            let db = TestDb::new();
            let agent = TestAgent::new("web", "user123", USR_AUDIENCE);

            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get db connection");

            let room = shared_helpers::insert_room(&conn);

            let event = shared_helpers::create_event(
                &conn,
                &room,
                1_000_000_000,
                "message",
                json!({"message": "m1"}),
            );

            // we had three unread notifications
            let notif1 = shared_helpers::insert_chat_notification(
                &conn,
                agent.agent_id(),
                room.id(),
                20,
                3,
                None,
            );
            let notif2 = shared_helpers::insert_chat_notification(
                &conn,
                agent.agent_id(),
                room.id(),
                40,
                5,
                None,
            );

            drop(conn);
            let notifications = super::call(db.connection_pool(), &event)
                .expect("incrementing notifications failed");

            // only one counter should be updated
            assert_eq!(notifications.len(), 1);

            // we should've updated proper counter
            assert_eq!(notifications[0].id(), notif1.id());
            // now we should have 4 of them
            assert_eq!(notifications[0].value(), 4);

            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get db connection");

            let notif2_ = FindQuery::new(notif2.id())
                .execute(&conn)
                .expect("Failed to fetch notification");
            // second counter stayed the same
            assert_eq!(notif2.value(), notif2_.value());
        });
    }
}
