use std::sync::Arc;

use svc_agent::{queue_counter::QueueCounterHandle, AgentId};
use svc_authz::ClientMap as Authz;

use crate::cache::AppCache;
use crate::config::Config;
use crate::db::ConnectionPool as Db;

#[derive(Clone)]
pub(crate) struct AppContext {
    config: Arc<Config>,
    authz: Authz,
    db: Db,
    agent_id: AgentId,
    queue_counter: Option<QueueCounterHandle>,
    cache: Option<AppCache>,
}

impl AppContext {
    pub(crate) fn new(config: Config, authz: Authz, db: Db) -> Self {
        let agent_id = AgentId::new(&config.agent_label, config.id.to_owned());

        Self {
            queue_counter: None,
            cache: None,
            config: Arc::new(config),
            authz,
            db,
            agent_id,
        }
    }

    pub(crate) fn add_queue_counter(self, qc: QueueCounterHandle) -> Self {
        Self {
            queue_counter: Some(qc),
            ..self
        }
    }

    pub(crate) fn add_app_cache(self, cache: AppCache) -> Self {
        Self {
            cache: Some(cache),
            ..self
        }
    }
}

pub(crate) trait Context: Sync {
    fn authz(&self) -> &Authz;
    fn config(&self) -> &Config;
    fn db(&self) -> &Db;
    fn agent_id(&self) -> &AgentId;
    fn queue_counter(&self) -> &Option<QueueCounterHandle>;
    fn app_cache(&self) -> &Option<AppCache>;
}

impl Context for AppContext {
    fn authz(&self) -> &Authz {
        &self.authz
    }

    fn config(&self) -> &Config {
        &self.config
    }

    fn db(&self) -> &Db {
        &self.db
    }

    fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    fn queue_counter(&self) -> &Option<QueueCounterHandle> {
        &self.queue_counter
    }

    fn app_cache(&self) -> &Option<AppCache> {
        &self.cache
    }
}
