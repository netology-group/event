extern crate event_benchmark;

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use clap::{value_t, App, Arg};
use log::info;
use quantiles::ckms::CKMS;
use serde_json::json;
use svc_agent::{
    mqtt::{
        AgentBuilder, AgentConfig, AgentNotification, ConnectionMode, IncomingEvent,
        IncomingMessage, QoS, ResponseStatus,
    },
    AccountId, AgentId, Subscription,
};
use uuid::Uuid;

use event_benchmark::{types::*, Interval, TestAgent};

///////////////////////////////////////////////////////////////////////////////

const CKMS_ERROR: f64 = 0.001;

/// Listens to event.create events in all rooms and remembers their processing times.
struct StatsAgent {
    is_running: AtomicBool,
    processing_times: Mutex<CKMS<u32>>,
    processing_times_proc: Mutex<CKMS<u32>>,
    processing_times_send: Mutex<CKMS<u32>>,
}

impl StatsAgent {
    fn new() -> Self {
        Self {
            is_running: AtomicBool::new(false),
            processing_times: Mutex::new(CKMS::<u32>::new(CKMS_ERROR)),
            processing_times_proc: Mutex::new(CKMS::<u32>::new(CKMS_ERROR)),
            processing_times_send: Mutex::new(CKMS::<u32>::new(CKMS_ERROR)),
        }
    }

    async fn start(&self, config: &AgentConfig, id: AgentId, service_account_id: &AccountId) {
        // Start agent.
        let (mut agent, rx) = AgentBuilder::new(id.clone(), API_VERSION)
            .connection_mode(ConnectionMode::Observer)
            .start(&config)
            .expect("Failed to start agent");

        // Subscribe to the service responses.
        let subscription =
            Subscription::broadcast_events(service_account_id, API_VERSION, "rooms/+/events");

        agent
            .subscribe(&subscription, QoS::AtLeastOnce, None)
            .expect("Error subscribing to broadcast events");

        // Set running and wait for notifications until stop.
        self.is_running.store(true, Ordering::SeqCst);
        let mut processing_times = self.processing_times.lock().expect("Failed to obtain lock");
        let mut processing_times_proc = self
            .processing_times_proc
            .lock()
            .expect("Failed to obtain lock");

        let mut processing_times_send = self
            .processing_times_send
            .lock()
            .expect("Failed to obtain lock");

        while self.is_running.load(Ordering::SeqCst) {
            if let Ok(AgentNotification::Message(Ok(IncomingMessage::Event(ev)), _)) = rx.try_recv()
            {
                // Remember processing time for event.create notification.
                if ev.properties().label() != Some("event.create") {
                    continue;
                }

                let evp_value =
                    serde_json::to_value(ev.properties()).expect("Failed to dump event properties");

                let broker_processing_timestamp = evp_value
                    .get("broker_processing_timestamp")
                    .expect("Missing broker_processing_timestamp in event properties")
                    .as_str()
                    .expect("Failed to cast broker_processing_timestamp as string")
                    .parse::<u64>()
                    .expect("Failed to parse broker_processing_timestamp");

                let broker_timestamp = evp_value
                    .get("broker_timestamp")
                    .expect("Missing broker_timestamp in event properties")
                    .as_str()
                    .expect("Failed to cast broker_timestamp as string")
                    .parse::<u64>()
                    .expect("Failed to parse broker_timestamp");

                let initial_timestamp = evp_value
                    .get("initial_timestamp")
                    .expect("Missing initial_timestamp in event properties")
                    .as_str()
                    .expect("Failed to cast initial_timestamp as string")
                    .parse::<u64>()
                    .expect("Failed to parse initial_timestamp");

                let timestamp = evp_value
                    .get("timestamp")
                    .expect("Missing timestamp in event properties")
                    .as_str()
                    .expect("Failed to cast timestamp as string")
                    .parse::<u64>()
                    .expect("Failed to parse timestamp");

                let processing_time = evp_value
                    .get("processing_time")
                    .expect("Missing processing_time in event properties")
                    .as_str()
                    .expect("Failed to cast processing_time as string")
                    .parse::<u64>()
                    .expect("Failed to parse processing_time");

                if initial_timestamp > broker_timestamp {
                    println!("{} {}", broker_timestamp, initial_timestamp);
                }

                (*processing_times).insert((broker_timestamp - initial_timestamp) as u32);
                (*processing_times_proc).insert(processing_time as u32);
                (*processing_times_send).insert((broker_processing_timestamp - timestamp) as u32);
            }
        }
    }

    fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
    }

    fn with_processing_times<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&CKMS<u32>) -> R,
    {
        let processing_times = self
            .processing_times
            .lock()
            .expect("Failed to obtain lock because stats agent failed during data collection");

        fun(&*processing_times)
    }

    fn with_processing_times_proc<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&CKMS<u32>) -> R,
    {
        let processing_times = self
            .processing_times_proc
            .lock()
            .expect("Failed to obtain lock because stats agent failed during data collection");

        fun(&*processing_times)
    }

    fn with_processing_times_send<F, R>(&self, fun: F) -> R
    where
        F: FnOnce(&CKMS<u32>) -> R,
    {
        let processing_times = self
            .processing_times_send
            .lock()
            .expect("Failed to obtain lock because stats agent failed during data collection");

        fun(&*processing_times)
    }
}

///////////////////////////////////////////////////////////////////////////////
#[async_std::main]
async fn main() {
    env_logger::init();

    let matches = App::new("event.create benchmark")
        .about("Creates N rooms with M online agents and makes R event.create RPS as each.")
        .arg(
            Arg::with_name("mqtt-host")
                .short("H")
                .long("mqtt-host")
                .help("Broker connection host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mqtt-port")
                .short("P")
                .long("mqtt-port")
                .help("Broker connection port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mqtt-password")
                .short("W")
                .long("mqtt-password")
                .help("Broker connection password")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("account-id")
                .short("A")
                .long("account-id")
                .help("Account ID with which agents connect. Must be allowed to connect in observer mode.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("service-account-id")
                .short("S")
                .long("service-account-id")
                .help("Event service account ID")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("audience")
                .short("a")
                .long("audience")
                .help("Audience to create rooms within. Default is the audience from account-id.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("rooms")
                .short("r")
                .long("rooms")
                .help("Number of rooms")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("agents")
                .short("u")
                .long("agents")
                .help("Number of agents per room")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("rate")
                .short("R")
                .long("rate")
                .help("Number of event.create RPS to make by each agent")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tag")
                .short("t")
                .long("tag")
                .help("Tag value to mark rooms for this run")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("persistent")
                .long("persistent")
                .help("Events persistance")
        )
        .get_matches();

    let mqtt_host = matches.value_of("mqtt-host").unwrap_or("0.0.0.0");
    let mqtt_port = value_t!(matches, "mqtt-port", u16).unwrap_or(1883);
    let mqtt_password = matches.value_of("mqtt-password");

    let account_id = AccountId::from_str(
        matches
            .value_of("account-id")
            .unwrap_or("test.dev.usr.example.org"),
    )
    .expect("Failed to parse account ID");

    let service_account_id = AccountId::from_str(
        matches
            .value_of("service-account-id")
            .unwrap_or("event.dev.svc.example.org"),
    )
    .expect("Failed to parse service account ID");

    let audience = matches
        .value_of("audience")
        .unwrap_or_else(|| account_id.audience());

    let rooms_count = value_t!(matches, "rooms", usize).unwrap_or(1);
    let agents_per_room = value_t!(matches, "agents", usize).unwrap_or(1);
    let rate = value_t!(matches, "rate", u64).unwrap_or(1);
    let tag = matches.value_of("tag").unwrap_or("benchmark");
    let persistance = value_t!(matches, "persistance", bool).unwrap_or(true);

    ///////////////////////////////////////////////////////////////////////////


    let agent_config_json = json!({
        "uri": format!("{}:{}", mqtt_host, mqtt_port),
        "incoming_message_queue_size": 1_000_000,
        "outgoing_message_queue_size": 1_000_000,
    });

    let mut agent_config = serde_json::from_value::<AgentConfig>(agent_config_json)
        .expect("Failed to parse agent config");

    if let Some(ref password) = mqtt_password {
        agent_config.set_password(password);
    }

    info!("Connecting stats agent.");
    let stats_agent_id = AgentId::new("stats", account_id.clone());
    let stats_agent = Arc::new(StatsAgent::new());
    let stats_agent_clone = stats_agent.clone();
    let agent_config_clone = agent_config.clone();
    let service_account_id_clone = service_account_id.clone();

    async_std::task::spawn(async move {
        stats_agent_clone
            .start(
                &agent_config_clone,
                stats_agent_id,
                &service_account_id_clone,
            )
            .await;
    });

    ///////////////////////////////////////////////////////////////////////////

    info!("Connecting {} agents.", rooms_count * agents_per_room);

    let rooms_agents = (0..rooms_count)
        .map(|room_idx| {
            (0..agents_per_room)
                .map(|agent_idx| {
                    // Start agent with service subscription.
                    let agent_label = format!("bench-{}-{}", room_idx, agent_idx);
                    let agent_id = AgentId::new(&agent_label, account_id.clone());
                    TestAgent::start(&agent_config, agent_id, &service_account_id)
                })
                .collect::<Vec<TestAgent>>()
        })
        .collect::<Vec<Vec<TestAgent>>>();

    ///////////////////////////////////////////////////////////////////////////

    info!("Creating and entering {} rooms.", rooms_count);

    let registry = rooms_agents
        .into_iter()
        .map(|room_agents| async {
            // Create room as the first agent.
            let now = Utc::now().timestamp();

            let response = room_agents[0].request::<RoomCreateRequest, RoomCreateResponse>(
                "room.create",
                RoomCreateRequest {
                    audience: audience.to_owned(),
                    time: (now, now + ROOM_DURATION),
                    tags: json!({ "benchmark": tag }),
                },
            ).await;

            if response.properties().status() != ResponseStatus::CREATED {
                panic!("Failed to create room");
            }

            let room_id = response.payload().id;

            // Enter room as each agent.
            for agent in &room_agents {
                let response = agent.request::<RoomEnterRequest, RoomEnterResponse>(
                    "room.enter",
                    RoomEnterRequest { id: room_id },
                ).await;

                if response.properties().status() != ResponseStatus::ACCEPTED {
                    panic!("Failed to enter room");
                }

                // Wait for entrance notification.
                agent.wait_for_event("room.enter", |event: &IncomingEvent<RoomEnterEvent>| {
                    event.payload().id == room_id && &event.payload().agent_id == agent.id()
                });
            }

            (room_id, room_agents)
        });

    let registry: Vec<(Uuid, Vec<TestAgent>)> = futures::future::join_all(registry).await;

    ///////////////////////////////////////////////////////////////////////////

    info!(
        "Creating {} events per second for each agent. Stop with Ctrl+C.",
        rate
    );

    println!("Starting bench");

    let mut intervals = Vec::with_capacity(rooms_count * agents_per_room);

    // Run event creation loop for each agent in each room.
    for (room_id, agents) in registry {
        for agent in agents {
            let interval = Arc::new(Interval::new(rate));
            let interval_clone = interval.clone();

            async_std::task::spawn(async move {
                interval_clone
                    .run(|| {
                        info!("Create event");

                        agent.request_nowait(
                            "event.create",
                            EventCreateRequest {
                                room_id,
                                kind: String::from("message"),
                                set: None,
                                label: None,
                                data: json!({ "text": "hello" }),
                                is_persistent: persistance,
                            },
                        )
                    })
                    .await;
            });

            intervals.push(interval);
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    // Wait for Ctrl + C to stop.
    let is_running = Arc::new(AtomicBool::new(true));
    let is_running_clone = is_running.clone();

    ctrlc::set_handler(move || {
        for interval in &intervals {
            interval.stop();
        }

        let old_running = is_running_clone.swap(false, Ordering::SeqCst);
        if old_running {
            println!("Stopping, please wait a bit");
        }
    })
    .expect("Failed to set Ctrl+C handler");

    async_std::task::sleep(std::time::Duration::from_secs(30)).await;

    ///////////////////////////////////////////////////////////////////////////

    // Stop collecting stats to unlock.
    stats_agent.stop();

    // Calculate and print results.
    stats_agent.with_processing_times(|times| {
        if times.count() > 0 {
            for q in vec![0.5, 0.75, 0.9, 0.95, 0.99, 1.0] {
                if let Some((a, b)) = times.query(q) {
                    println!("Q{}: {}, {}", q, a, b);
                }
            }
        } else {
            println!("No events captured");
        }
    });

    println!("\n\n processing time:\n");

    stats_agent.with_processing_times_proc(|times| {
        if times.count() > 0 {
            for q in vec![0.5, 0.75, 0.9, 0.95, 0.99, 1.0] {
                if let Some((a, b)) = times.query(q) {
                    println!("Q{}: {}, {}", q, a, b);
                }
            }
        } else {
            println!("No events captured");
        }
    });


    println!("\n\n time between app and broker:\n");

    stats_agent.with_processing_times_send(|times| {
        if times.count() > 0 {
            for q in vec![0.5, 0.75, 0.9, 0.95, 0.99, 1.0] {
                if let Some((a, b)) = times.query(q) {
                    println!("Q{}: {}, {}", q, a, b);
                }
            }
        } else {
            println!("No events captured");
        }
    });
}
