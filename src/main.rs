#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_derive_enum;

use std::env::var;

use anyhow::Result;
use dotenv::dotenv;
use svc_authz::cache::{create_pool, Cache};
use tracing_subscriber::FmtSubscriber;

#[async_std::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::with_format(
            "%Y-%m-%d %H:%M:%S%.6f".to_string(),
        ))
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let ((db, db_pool_stats), maybe_ro_db) = {
        let url = var("DATABASE_URL").expect("DATABASE_URL must be specified");

        let size = var("DATABASE_POOL_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting DATABASE_POOL_SIZE variable into u32")
            })
            .unwrap_or(5);

        let idle_size = var("DATABASE_POOL_IDLE_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting DATABASE_POOL_IDLE_SIZE variable into u32")
            })
            .ok();

        let timeout = var("DATABASE_POOL_TIMEOUT")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting DATABASE_POOL_TIMEOUT variable into u64")
            })
            .unwrap_or(5);

        let db = crate::db::create_pool(&url, size, idle_size, timeout, true);

        let maybe_ro_db = var("READONLY_DATABASE_URL")
            .ok()
            .map(|ro_url| crate::db::create_pool(&ro_url, size, idle_size, timeout, true));

        (db, maybe_ro_db)
    };

    let (redis_pool, authz_cache) = if let Some("1") = var("CACHE_ENABLED").ok().as_deref() {
        let url = var("CACHE_URL").expect("CACHE_URL must be specified");

        let size = var("CACHE_POOL_SIZE")
            .map(|val| {
                val.parse::<u32>()
                    .expect("Error converting CACHE_POOL_SIZE variable into u32")
            })
            .unwrap_or_else(|_| 5);

        let timeout = var("CACHE_POOL_TIMEOUT")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting CACHE_POOL_TIMEOUT variable into u64")
            })
            .unwrap_or_else(|_| 5);

        let expiration_time = var("CACHE_EXPIRATION_TIME")
            .map(|val| {
                val.parse::<u64>()
                    .expect("Error converting CACHE_EXPIRATION_TIME variable into u64")
            })
            .unwrap_or_else(|_| 300);

        let pool = create_pool(&url, size, timeout);
        let cache = Cache::new(pool.clone(), expiration_time);
        (Some(pool), Some(cache))
    } else {
        (None, None)
    };

    let (maybe_ro_db, maybe_ro_db_pool_stats) = match maybe_ro_db {
        Some((db, pool_stats)) => (Some(db), Some(pool_stats)),
        None => (None, None),
    };

    app::run(
        db,
        maybe_ro_db,
        redis_pool,
        authz_cache,
        db_pool_stats,
        maybe_ro_db_pool_stats,
    )
    .await
}

mod app;
mod config;
mod db;
mod profiler;
#[allow(unused_imports)]
#[rustfmt::skip]
mod schema;
mod serde;
#[cfg(test)]
mod test_helpers;
