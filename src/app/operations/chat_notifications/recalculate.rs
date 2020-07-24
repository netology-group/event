use anyhow::{Context, Result};
use chrono::Utc;
use diesel::pg::PgConnection;
use log::info;
use svc_agent::AccountId;
use uuid::Uuid;

use crate::db::chat_notification::Object as ChatNotification;
use crate::db::room::Object as Room;
use crate::db::ConnectionPool as Db;

pub(crate) fn call(
    db: &Db,
    room: &Room,
    last_seen_id: Uuid,
    counter_owner: &AccountId,
    event_kind: &str,
) -> Result<Vec<ChatNotification>> {
    info!(
        "Recalculate chat notifications task, room = {}, last_seen_id = {}",
        room.id(),
        last_seen_id
    );

    let start_timestamp = Utc::now();
    let conn = db.get()?;

    let result = recalculate_counter(&conn, room.id(), last_seen_id, counter_owner, event_kind);

    info!(
        "Recalculate chat notifications task successfully finished for room_id = '{}', last_seen_id = {}, duration = {} ms",
        room.id(),
        last_seen_id,
        (Utc::now() - start_timestamp).num_milliseconds()
    );

    result
}

fn recalculate_counter(
    conn: &PgConnection,
    room_id: Uuid,
    last_seen_id: Uuid,
    counter_owner: &AccountId,
    event_kind: &str,
) -> Result<Vec<ChatNotification>> {
    use diesel::prelude::*;
    use diesel::sql_types::{Uuid, VarChar};

    use crate::db::sql::Account_id;

    diesel::sql_query(RECALC_COUNTERS)
        .bind::<Uuid, _>(room_id)
        .bind::<Uuid, _>(last_seen_id)
        .bind::<VarChar, _>(event_kind)
        .bind::<Account_id, _>(counter_owner)
        .get_results(conn)
        .with_context(|| format!("Failed setting counter",))
}

const RECALC_COUNTERS: &str = r#"
UPDATE chat_notification
SET
    value = q.value,
    last_seen_id = $2
FROM (
        SELECT COUNT(id) AS value, priority
        FROM event
        WHERE room_id = $1
        AND occurred_at > (SELECT occurred_at FROM event WHERE room_id = $1 AND id = $2)
        AND set IS NOT NULL
        AND kind = $3
        AND (created_by).account_id <> $4
        GROUP BY priority
    ) AS q
WHERE chat_notification.priority = q.priority AND account_id = $4 AND room_id = $1
RETURNING *
"#;

#[cfg(test)]
mod tests {
    use serde_json::json;
    use svc_authn::Authenticable;

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

            shared_helpers::create_event(
                &conn,
                &room,
                2_000_000_000,
                "message",
                json!({"message": "m2"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                3_000_000_000,
                "message",
                json!({"message": "m3"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                4_000_000_000,
                "message",
                json!({"message": "m4"}),
            );

            // we had four unread notifications
            let notif = shared_helpers::insert_chat_notification(
                &conn,
                agent.agent_id(),
                room.id(),
                20,
                4,
                None,
            );

            drop(conn);
            // we mark first one as read
            let notifications = super::call(
                db.connection_pool(),
                &room,
                event.id(),
                agent.agent_id().as_account_id(),
                "message",
            )
            .expect("incrementing notifications failed");

            // only one counter should be updated
            assert_eq!(notifications.len(), 1);

            // we should've updated proper counter
            assert_eq!(notifications[0].id(), notif.id());
            // now we should have 3 of them
            assert_eq!(notifications[0].value(), 3);
        });
    }
}
