-- This file should undo anything in `up.sql`
DROP TABLE chat_notification;
ALTER TABLE event DROP COLUMN priority;
