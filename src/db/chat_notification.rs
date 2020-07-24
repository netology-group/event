use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use diesel::{pg::PgConnection, result::Error};
use serde_derive::{Deserialize, Serialize};
use svc_agent::AccountId;
use uuid::Uuid;

use super::room::Object as Room;
use crate::schema::chat_notification;

pub(crate) const DEFAULT_PRIORITY: i32 = 20;

#[derive(
    Clone, Debug, Deserialize, Serialize, Identifiable, Associations, Queryable, QueryableByName,
)]
#[table_name = "chat_notification"]
#[belongs_to(Room, foreign_key = "room_id")]
pub(crate) struct Object {
    id: Uuid,
    account_id: AccountId,
    room_id: Uuid,
    #[serde(with = "ts_milliseconds")]
    created_at: DateTime<Utc>,
    priority: i32,
    value: i32,
    last_seen_id: Option<Uuid>,
}

impl Object {
    #[cfg(test)]
    pub fn value(&self) -> i32 {
        self.value
    }

    #[cfg(test)]
    pub fn id(&self) -> Uuid {
        self.id
    }
}
///////////////////////////////////////////////////////////////////////////////

pub(crate) struct ListQuery<'a> {
    account_id: &'a AccountId,
}

impl<'a> ListQuery<'a> {
    pub(crate) fn new(account_id: &'a AccountId) -> Self {
        Self { account_id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Vec<Object>, Error> {
        use diesel::prelude::*;

        let q = chat_notification::table
            .filter(chat_notification::account_id.eq(&self.account_id))
            .into_boxed();

        q.get_results(conn)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Insertable)]
#[table_name = "chat_notification"]
pub(crate) struct InsertQuery<'a> {
    account_id: &'a AccountId,
    room_id: Uuid,
    priority: Option<i32>,
    value: Option<i32>,
    last_seen_id: Option<Uuid>,
}

impl<'a> InsertQuery<'a> {
    pub(crate) fn new(account_id: &'a AccountId, room_id: Uuid) -> Self {
        Self {
            account_id,
            room_id,
            priority: Some(DEFAULT_PRIORITY),
            value: None,
            last_seen_id: None,
        }
    }

    pub(crate) fn priority(self, priority: i32) -> Self {
        Self {
            priority: Some(priority),
            ..self
        }
    }

    pub(crate) fn value(self, value: i32) -> Self {
        Self {
            value: Some(value),
            ..self
        }
    }

    pub(crate) fn last_seen_id(self, last_seen_id: Uuid) -> Self {
        Self {
            last_seen_id: Some(last_seen_id),
            ..self
        }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use crate::diesel::RunQueryDsl;
        use crate::schema::chat_notification::dsl::chat_notification;

        diesel::insert_into(chat_notification)
            .values(self)
            .on_conflict_do_nothing()
            .get_result(conn)
    }
}

#[cfg(test)]
pub(crate) struct FindQuery {
    id: Uuid,
}

#[cfg(test)]
impl FindQuery {
    pub(crate) fn new(id: Uuid) -> Self {
        Self { id }
    }

    pub(crate) fn execute(&self, conn: &PgConnection) -> Result<Object, Error> {
        use diesel::prelude::*;

        let q = chat_notification::table
            .filter(chat_notification::id.eq(&self.id))
            .into_boxed();

        q.get_result(conn)
    }
}
