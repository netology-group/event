use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context as AnyhowContext, Result};
use async_std::task;
use chrono::Utc;
use futures::StreamExt;
use log::{error, info, warn};
use serde_json::json;
use svc_agent::mqtt::{
    Agent, AgentBuilder, AgentNotification, ConnectionMode, OutgoingRequest,
    OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
};
use svc_agent::{AccountId, AgentId, Authenticable, SharedGroup, Subscription};
use svc_authn::token::jws_compact;
use svc_authz::cache::{Cache as AuthzCache, ConnectionPool as RedisConnectionPool};
use svc_error::{extension::sentry, Error as SvcError};

use crate::app::context::Context;
use crate::app::metrics::StatsCollector;
use crate::config::{self, Config, KruonisConfig};
use crate::db::ConnectionPool;
use context::AppContextBuilder;
use message_handler::MessageHandler;

pub(crate) const API_VERSION: &str = "v1";

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn run(
    db: ConnectionPool,
    ro_db: Option<ConnectionPool>,
    redis_pool: Option<RedisConnectionPool>,
    authz_cache: Option<AuthzCache>,
    db_pool_stats: StatsCollector,
    ro_db_pool_stats: Option<StatsCollector>,
) -> Result<()> {
    // Config
    let config = config::load().context("Failed to load config")?;
    info!("App config: {:?}", config);

    // Agent
    let agent_id = AgentId::new(&config.agent_label, config.id.clone());
    info!("Agent id: {:?}", &agent_id);

    let token = jws_compact::TokenBuilder::new()
        .issuer(&agent_id.as_account_id().audience().to_string())
        .subject(&agent_id)
        .key(config.id_token.algorithm, config.id_token.key.as_slice())
        .build()
        .context("Error creating an id token")?;

    let mut agent_config = config.mqtt.clone();
    agent_config.set_password(&token);

    let (mut agent, rx) = AgentBuilder::new(agent_id.clone(), API_VERSION)
        .connection_mode(ConnectionMode::Service)
        .start(&agent_config)
        .context("Failed to create an agent")?;

    // Message loop for incoming messages of MQTT Agent
    let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<AgentNotification>();

    thread::Builder::new()
        .name("event-notifications-loop".to_owned())
        .spawn(move || {
            for message in rx {
                if mq_tx.unbounded_send(message).is_err() {
                    error!("Error sending message to the internal channel");
                }
            }
        })
        .expect("Failed to start event notifications loop");

    // Authz
    let authz = svc_authz::ClientMap::new(&config.id, authz_cache, config.authz.clone())
        .context("Error converting authz config to clients")?;

    // Sentry
    if let Some(sentry_config) = config.sentry.as_ref() {
        svc_error::extension::sentry::init(sentry_config);
    }

    // Subscribe to topics
    subscribe(&mut agent, &agent_id, &config)?;

    // Context
    let context_builder = AppContextBuilder::new(config, authz, db);

    let context_builder = match ro_db {
        Some(db) => context_builder.ro_db(db),
        None => context_builder,
    };

    let context_builder = match redis_pool {
        Some(pool) => context_builder.redis_pool(pool),
        None => context_builder,
    };

    let context_builder = context_builder.db_pool_stats(db_pool_stats);

    let context_builder = match ro_db_pool_stats {
        Some(stats) => context_builder.ro_db_pool_stats(stats),
        None => context_builder,
    };

    let context = context_builder
        .queue_counter(agent.get_queue_counter())
        .build();

    // Message handler
    let message_handler = Arc::new(MessageHandler::new(agent, context));

    // Message loop
    let term_check_period = Duration::from_secs(1);
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGTERM, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::SIGINT, Arc::clone(&term))?;

    while !term.load(Ordering::Relaxed) {
        let fut = async_std::future::timeout(term_check_period, mq_rx.next());

        if let Ok(Some(message)) = fut.await {
            let message_handler = message_handler.clone();

            task::spawn_blocking(move || match message {
                AgentNotification::Message(ref message, _) => {
                    let req_uuid = uuid::Uuid::new_v4().to_string();
                    async_std::task::block_on(message_handler.handle(req_uuid, message));
                }
                AgentNotification::Disconnection => error!("Disconnected from broker"),
                AgentNotification::Reconnection => {
                    error!("Reconnected to broker");

                    resubscribe(
                        &mut message_handler.agent().to_owned(),
                        message_handler.context().agent_id(),
                        message_handler.context().config(),
                    );
                }
                AgentNotification::Puback(_) => (),
                AgentNotification::Pubrec(_) => (),
                AgentNotification::Pubcomp(_) => (),
                AgentNotification::Suback(_) => (),
                AgentNotification::Unsuback(_) => (),
                AgentNotification::Abort(err) => {
                    error!("MQTT client aborted: {}", err);
                }
            });
        }
    }

    Ok(())
}

fn subscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) -> Result<()> {
    let group = SharedGroup::new("loadbalancer", agent_id.as_account_id().clone());

    // Multicast requests
    agent
        .subscribe(
            &Subscription::multicast_requests(Some(API_VERSION)),
            QoS::AtMostOnce,
            Some(&group),
        )
        .context("Error subscribing to multicast requests")?;

    // Unicast requests
    agent
        .subscribe(&Subscription::unicast_requests(), QoS::AtMostOnce, None)
        .context("Error subscribing to unicast requests")?;

    // Kruonis
    if let KruonisConfig {
        id: Some(ref kruonis_id),
    } = config.kruonis
    {
        subscribe_to_kruonis(kruonis_id, agent)?;
    }

    Ok(())
}

fn subscribe_to_kruonis(kruonis_id: &AccountId, agent: &mut Agent) -> Result<()> {
    let timing = ShortTermTimingProperties::new(Utc::now());

    let topic = Subscription::unicast_requests_from(kruonis_id)
        .subscription_topic(agent.id(), API_VERSION)
        .context("Failed to build subscription topic")?;

    let props = OutgoingRequestProperties::new("kruonis.subscribe", &topic, "", timing);
    let event = OutgoingRequest::multicast(json!({}), props, kruonis_id);

    agent.publish(event).context("Failed to publish message")?;
    Ok(())
}

fn resubscribe(agent: &mut Agent, agent_id: &AgentId, config: &Config) {
    if let Err(err) = subscribe(agent, agent_id, config) {
        let err = format!("Failed to resubscribe after reconnection: {}", err);
        error!("{}", err);

        let svc_error = SvcError::builder()
            .kind("resubscription_error", "Resubscription error")
            .detail(&err)
            .build();

        sentry::send(svc_error)
            .unwrap_or_else(|err| warn!("Error sending error to Sentry: {}", err));
    }
}

pub(crate) mod context;
pub(crate) mod endpoint;
pub(crate) mod message_handler;
pub(crate) mod metrics;
pub(crate) mod operations;
