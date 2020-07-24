-- Your SQL goes here

CREATE TABLE chat_notification (
    id uuid DEFAULT gen_random_uuid(),
    account_id account_id NOT NULL,
    room_id uuid NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority INTEGER NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    last_seen_id uuid,

    FOREIGN KEY (room_id) REFERENCES room (id) ON DELETE CASCADE,
    UNIQUE (account_id, room_id, priority),
    PRIMARY KEY (id)
);

ALTER TABLE event ADD COLUMN priority INTEGER;
