use diesel::pg::PgConnection;
use serde_json::Value as JsonValue;
use svc_agent::{AccountId, AgentId, Authenticable};
use uuid::Uuid;

use crate::db;

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Room {
    audience: Option<String>,
    time: Option<db::room::Time>,
    tags: Option<JsonValue>,
}

impl Room {
    pub(crate) fn new() -> Self {
        Self {
            audience: None,
            time: None,
            tags: None,
        }
    }

    pub(crate) fn audience(self, audience: &str) -> Self {
        Self {
            audience: Some(audience.to_owned()),
            ..self
        }
    }

    pub(crate) fn time(self, time: db::room::Time) -> Self {
        Self {
            time: Some(time),
            ..self
        }
    }

    pub(crate) fn tags(self, tags: &JsonValue) -> Self {
        Self {
            tags: Some(tags.to_owned()),
            ..self
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::room::Object {
        let audience = self.audience.expect("Audience not set");
        let time = self.time.expect("Time not set");

        let mut query = db::room::InsertQuery::new(&audience, time);

        if let Some(tags) = self.tags {
            query = query.tags(tags)
        }

        query.execute(conn).expect("Failed to insert room")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Agent {
    agent_id: Option<AgentId>,
    room_id: Option<Uuid>,
    status: Option<db::agent::Status>,
}

impl Agent {
    pub(crate) fn new() -> Self {
        Self {
            agent_id: None,
            room_id: None,
            status: None,
        }
    }

    pub(crate) fn agent_id(self, agent_id: AgentId) -> Self {
        Self {
            agent_id: Some(agent_id),
            ..self
        }
    }

    pub(crate) fn room_id(self, room_id: Uuid) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub(crate) fn status(self, status: db::agent::Status) -> Self {
        Self {
            status: Some(status),
            ..self
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::agent::Object {
        let agent_id = self.agent_id.expect("Agent ID not set");
        let room_id = self.room_id.expect("Room ID not set");

        let mut query = db::agent::InsertQuery::new(&agent_id, room_id);

        if let Some(status) = self.status {
            query = query.status(status);
        }

        query.execute(conn).expect("Failed to insert agent")
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Event {
    room_id: Option<Uuid>,
    kind: Option<String>,
    set: Option<String>,
    label: Option<String>,
    data: Option<JsonValue>,
    occurred_at: Option<i64>,
    created_by: Option<AgentId>,
}

impl Event {
    pub(crate) fn new() -> Self {
        Self {
            room_id: None,
            kind: None,
            set: None,
            label: None,
            data: None,
            occurred_at: None,
            created_by: None,
        }
    }

    pub(crate) fn room_id(self, room_id: Uuid) -> Self {
        Self {
            room_id: Some(room_id),
            ..self
        }
    }

    pub(crate) fn kind(self, kind: &str) -> Self {
        Self {
            kind: Some(kind.to_owned()),
            ..self
        }
    }

    pub(crate) fn set(self, set: &str) -> Self {
        Self {
            set: Some(set.to_owned()),
            ..self
        }
    }

    pub(crate) fn label(self, label: &str) -> Self {
        Self {
            label: Some(label.to_owned()),
            ..self
        }
    }

    pub(crate) fn data(self, data: &JsonValue) -> Self {
        Self {
            data: Some(data.to_owned()),
            ..self
        }
    }

    pub(crate) fn occurred_at(self, occurred_at: i64) -> Self {
        Self {
            occurred_at: Some(occurred_at),
            ..self
        }
    }

    pub(crate) fn created_by(self, created_by: &AgentId) -> Self {
        Self {
            created_by: Some(created_by.to_owned()),
            ..self
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::event::Object {
        let room_id = self.room_id.expect("Room ID not set");
        let kind = self.kind.expect("Kind not set");
        let data = self.data.expect("Data not set");
        let occurred_at = self.occurred_at.expect("Occurrence date not set");
        let created_by = self.created_by.expect("Creator not set");

        let mut query =
            db::event::InsertQuery::new(room_id, &kind, &data, occurred_at, &created_by);

        if let Some(ref set) = self.set {
            query = query.set(set);
        }

        if let Some(ref label) = self.label {
            query = query.label(label);
        }

        query.execute(conn).expect("Failed to insert event")
    }
}

pub(crate) struct Edition {
    source_room_id: Uuid,
    created_by: AgentId,
}

impl Edition {
    pub(crate) fn new(source_room_id: Uuid, created_by: &AgentId) -> Self {
        Self {
            source_room_id,
            created_by: created_by.to_owned(),
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::edition::Object {
        let query = db::edition::InsertQuery::new(self.source_room_id, &self.created_by);
        query.execute(conn).expect("Failed to insert edition")
    }
}

pub(crate) struct Change {
    edition_id: Uuid,
    kind: crate::db::change::ChangeType,
    event_id: Option<Uuid>,
    event_data: Option<JsonValue>,
    event_kind: Option<String>,
    event_set: Option<String>,
    event_label: Option<String>,
    event_occurred_at: Option<i64>,
    event_created_by: Option<AgentId>,
}

impl Change {
    pub(crate) fn new(edition_id: Uuid, kind: crate::db::change::ChangeType) -> Self {
        Self {
            event_data: None,
            event_id: None,
            event_kind: None,
            event_set: None,
            event_label: None,
            event_occurred_at: None,
            event_created_by: None,
            edition_id,
            kind,
        }
    }

    pub(crate) fn event_id(self, event_id: Uuid) -> Self {
        Self {
            event_id: Some(event_id),
            ..self
        }
    }

    pub(crate) fn event_data(self, data: JsonValue) -> Self {
        Self {
            event_data: Some(data),
            ..self
        }
    }

    pub(crate) fn event_kind(self, kind: &str) -> Self {
        Self {
            event_kind: Some(kind.to_owned()),
            ..self
        }
    }

    pub(crate) fn event_set(self, set: &str) -> Self {
        Self {
            event_set: Some(set.to_owned()),
            ..self
        }
    }

    pub(crate) fn event_label(self, label: &str) -> Self {
        Self {
            event_label: Some(label.to_owned()),
            ..self
        }
    }

    pub(crate) fn event_occurred_at(self, occurred_at: i64) -> Self {
        Self {
            event_occurred_at: Some(occurred_at),
            ..self
        }
    }

    pub(crate) fn event_created_by(self, created_by: &AgentId) -> Self {
        Self {
            event_created_by: Some(created_by.to_owned()),
            ..self
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::change::Object {
        let query = db::change::InsertQuery::new(self.edition_id, self.kind);

        let query = if let Some(event_id) = self.event_id {
            query.event_id(event_id)
        } else {
            query
        };

        let query = if let Some(ref event_data) = self.event_data {
            query.event_data(event_data)
        } else {
            query
        };

        let query = if let Some(ref event_kind) = self.event_kind {
            query.event_kind(event_kind)
        } else {
            query
        };

        let query = query.event_set(&self.event_set);
        let query = query.event_label(&self.event_label);

        let query = if let Some(occurred_at) = self.event_occurred_at {
            query.event_occurred_at(occurred_at)
        } else {
            query
        };

        let query = if let Some(ref created_by) = self.event_created_by {
            query.event_created_by(created_by)
        } else {
            query
        };

        query.execute(conn).expect("Failed to insert edition")
    }
}

pub(crate) struct ChatNotification {
    account_id: AccountId,
    room_id: Uuid,
    priority: i32,
    value: i32,
    last_seen_id: Option<Uuid>,
}

impl ChatNotification {
    pub(crate) fn new(
        agent_id: &AgentId,
        room_id: Uuid,
        priority: i32,
        value: i32,
        last_seen_id: Option<Uuid>,
    ) -> Self {
        Self {
            room_id,
            priority,
            value,
            last_seen_id,
            account_id: agent_id.as_account_id().to_owned(),
        }
    }

    pub(crate) fn insert(self, conn: &PgConnection) -> db::chat_notification::Object {
        let query = db::chat_notification::InsertQuery::new(&self.account_id, self.room_id)
            .priority(self.priority)
            .value(self.value);
        let query = if let Some(last_seen_id) = self.last_seen_id {
            query.last_seen_id(last_seen_id)
        } else {
            query
        };
        query.execute(conn).expect("Failed to insert edition")
    }
}
