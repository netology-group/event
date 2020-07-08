use async_std::sync::{Arc, RwLock};
use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::db::room::Object as Room;

const MAX_COUNT: usize = 100;

type RoomCache = HashMap<Uuid, (Room, DateTime<Utc>)>;
type Author = Option<String>;
type AuthorKey = (Uuid, String, String);
type AuthorsCache = HashMap<AuthorKey, (Author, DateTime<Utc>)>;

#[derive(Clone)]
pub struct AppCache {
    rooms: Arc<RwLock<RoomCache>>,
    authors: Arc<RwLock<AuthorsCache>>,
    expiration_dur: Duration,
}

impl AppCache {
    pub fn new(expiration_secs: u64) -> Self {
        let expiration_dur = Duration::seconds(expiration_secs as i64);
        Self {
            rooms: Arc::new(RwLock::new(HashMap::new())),
            authors: Arc::new(RwLock::new(HashMap::new())),
            expiration_dur,
        }
    }

    pub(crate) async fn room(&self, room_id: Uuid) -> Option<Room> {
        let cache = self.rooms.read().await;
        match cache.get(&room_id) {
            Some((room, expiry)) => {
                if expiry < &Utc::now() {
                    return None;
                } else {
                    Some(room.clone())
                }
            }
            None => None,
        }
    }

    pub(crate) async fn insert_room(&self, room: Room) {
        let mut cache = self.rooms.write().await;

        let now = Utc::now();
        if cache.len() >= MAX_COUNT {
            cache.retain(|_key, (_room, ref expiry)| expiry > &now);
        }

        cache.insert(room.id(), (room, now + self.expiration_dur));
    }

    pub(crate) async fn author(
        &self,
        room_id: Uuid,
        set: &String,
        label: &String,
    ) -> Option<Author> {
        let cache = self.authors.read().await;
        let key = (room_id, set.to_owned(), label.to_owned());
        match cache.get(&key) {
            Some((author, expiry)) => {
                if expiry < &Utc::now() {
                    return None;
                } else {
                    Some(author.to_owned())
                }
            }
            None => None,
        }
    }

    pub(crate) async fn insert_author(
        &self,
        room_id: Uuid,
        set: String,
        label: String,
        author: Author,
    ) {
        let mut cache = self.authors.write().await;
        let key = (room_id, set, label);

        let now = Utc::now();
        if cache.len() >= MAX_COUNT {
            cache.retain(|_key, (_author, ref expiry)| expiry > &now);
        }

        cache.insert(key, (author, now + self.expiration_dur));
    }
}
