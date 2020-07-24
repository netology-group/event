table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    adjustment (room_id) {
        room_id -> Uuid,
        started_at -> Timestamptz,
        segments -> Array<Int8range>,
        offset -> Int8,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    agent (id) {
        id -> Uuid,
        agent_id -> Agent_id,
        room_id -> Uuid,
        status -> Agent_status,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    change (id) {
        id -> Uuid,
        edition_id -> Uuid,
        kind -> Change_type,
        event_id -> Nullable<Uuid>,
        event_kind -> Nullable<Text>,
        event_set -> Nullable<Text>,
        event_label -> Nullable<Text>,
        event_data -> Nullable<Jsonb>,
        event_occurred_at -> Nullable<Int8>,
        event_created_by -> Nullable<Agent_id>,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    chat_notification (id) {
        id -> Uuid,
        account_id -> Account_id,
        room_id -> Uuid,
        created_at -> Timestamptz,
        priority -> Int4,
        value -> Int4,
        last_seen_id -> Nullable<Uuid>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    edition (id) {
        id -> Uuid,
        source_room_id -> Uuid,
        created_by -> Agent_id,
        created_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    event (id) {
        id -> Uuid,
        room_id -> Uuid,
        kind -> Text,
        set -> Text,
        label -> Nullable<Text>,
        data -> Jsonb,
        occurred_at -> Int8,
        created_by -> Agent_id,
        created_at -> Timestamptz,
        deleted_at -> Nullable<Timestamptz>,
        original_occurred_at -> Int8,
        priority -> Nullable<Int4>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db::sql::*;

    room (id) {
        id -> Uuid,
        audience -> Text,
        source_room_id -> Nullable<Uuid>,
        time -> Tstzrange,
        tags -> Nullable<Json>,
        created_at -> Timestamptz,
    }
}

joinable!(adjustment -> room (room_id));
joinable!(agent -> room (room_id));
joinable!(change -> edition (edition_id));
joinable!(change -> event (event_id));
joinable!(chat_notification -> room (room_id));
joinable!(edition -> room (source_room_id));
joinable!(event -> room (room_id));

allow_tables_to_appear_in_same_query!(
    adjustment,
    agent,
    change,
    chat_notification,
    edition,
    event,
    room,
);
