use std::ops::Bound;

use chrono::{Duration, SubsecRound, Utc};
use diesel::pg::PgConnection;
use serde_json::{json, Value as JsonValue};
use svc_agent::{AccountId, AgentId};
use uuid::Uuid;

use crate::db::agent::{Object as Agent, Status as AgentStatus};
use crate::db::chat_notification::Object as ChatNotification;
use crate::db::edition::Object as Edition;
use crate::db::event::{InsertQuery as EventInsertQuery, Object as Event};
use crate::db::room::Object as Room;

use super::{factory, SVC_AUDIENCE, USR_AUDIENCE};

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn insert_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now),
            Bound::Excluded(now + Duration::hours(1)),
        ))
        .tags(&json!({ "webinar_id": "123" }))
        .insert(&conn)
}

pub(crate) fn insert_closed_room(conn: &PgConnection) -> Room {
    let now = Utc::now().trunc_subsecs(0);

    factory::Room::new()
        .audience(USR_AUDIENCE)
        .time((
            Bound::Included(now - Duration::hours(10)),
            Bound::Excluded(now - Duration::hours(8)),
        ))
        .tags(&json!({ "webinar_id": "123" }))
        .insert(&conn)
}

pub(crate) fn insert_agent(conn: &PgConnection, agent_id: &AgentId, room_id: Uuid) -> Agent {
    factory::Agent::new()
        .agent_id(agent_id.to_owned())
        .room_id(room_id)
        .status(AgentStatus::Ready)
        .insert(&conn)
}

pub(crate) fn insert_edition(conn: &PgConnection, room: &Room, agent_id: &AgentId) -> Edition {
    factory::Edition::new(room.id(), &agent_id).insert(&conn)
}

pub(crate) fn insert_chat_notification(
    conn: &PgConnection,
    agent_id: &AgentId,
    room_id: Uuid,
    priority: i32,
    value: i32,
    last_seen_id: Option<Uuid>,
) -> ChatNotification {
    factory::ChatNotification::new(agent_id, room_id, priority, value, last_seen_id).insert(&conn)
}

pub(crate) fn create_event(
    conn: &PgConnection,
    room: &Room,
    occurred_at: i64,
    kind: &str,
    data: JsonValue,
) -> Event {
    let created_by = AgentId::new("test", AccountId::new("test", SVC_AUDIENCE));

    let opened_at = match room.time() {
        (Bound::Included(opened_at), _) => *opened_at,
        _ => panic!("Invalid room time"),
    };

    EventInsertQuery::new(room.id(), kind, &data, occurred_at, &created_by)
        .created_at(opened_at + Duration::nanoseconds(occurred_at))
        .execute(conn)
        .expect("Failed to insert event")
}
