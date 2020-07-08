extern crate event_benchmark;

use std::str::FromStr;

use chrono::Utc;
use clap::{value_t, App, Arg};
use log::info;
use serde_json::json;
use svc_agent::{
    mqtt::{AgentConfig, IncomingEvent, ResponseStatus},
    AccountId, AgentId, Authenticable,
};

use event_benchmark::{types::*, TestAgent};

///////////////////////////////////////////////////////////////////////////////
#[async_std::main]
async fn main() {
    env_logger::init();

    let matches = App::new("state.read benchmark")
        .about("Creates a room with a number of events and measures state reading time.")
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
                .help("Account ID with which agents connect")
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
            Arg::with_name("sets")
                .short("s")
                .long("sets")
                .help("Number of sets to create")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("labels")
                .short("l")
                .long("labels")
                .help("Number of labels to create in each set")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("events")
                .short("e")
                .long("events")
                .help("Number of events to create for each label in each set")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tag")
                .short("t")
                .long("tag")
                .help("Tag value to mark the room for this run")
                .takes_value(true),
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

    let sets_count = value_t!(matches, "sets", usize).unwrap_or(1);
    let labels_count = value_t!(matches, "labels", usize).unwrap_or(1);
    let events_count = value_t!(matches, "events", usize).unwrap_or(1);
    let tag = matches.value_of("tag").unwrap_or("benchmark");

    ///////////////////////////////////////////////////////////////////////////

    let agent_config_json = json!({
        "uri": format!("{}:{}", mqtt_host, mqtt_port),
    });

    let mut agent_config = serde_json::from_value::<AgentConfig>(agent_config_json)
        .expect("Failed to parse agent config");

    if let Some(ref password) = mqtt_password {
        agent_config.set_password(password);
    }

    info!("Connecting agent.");
    let agent_id = AgentId::new("benchmark", account_id);

    let agent = TestAgent::start(
        &agent_config,
        agent_id.clone(),
        &service_account_id,
    );

    ///////////////////////////////////////////////////////////////////////////

    info!("Creating a room.");
    let now = Utc::now().timestamp();

    let response = agent.request::<RoomCreateRequest, RoomCreateResponse>(
        "room.create",
        RoomCreateRequest {
            audience: agent_id.as_account_id().audience().to_owned(),
            time: (now, now + ROOM_DURATION),
            tags: json!({ "benchmark": tag }),
        },
    ).await;

    if response.properties().status() != ResponseStatus::CREATED {
        panic!("Failed to create room");
    }

    let room_id = response.payload().id;

    ///////////////////////////////////////////////////////////////////////////

    info!("Entering the room.");

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

    ///////////////////////////////////////////////////////////////////////////

    info!(
        "Creating {} events for {} labels in {} sets.",
        events_count, labels_count, sets_count
    );

    for set_idx in 0..sets_count {
        for label_idx in 0..labels_count {
            for event_idx in 0..events_count {
                let response = agent.request::<EventCreateRequest, EventCreateResponse>(
                    "event.create",
                    EventCreateRequest {
                        room_id,
                        kind: String::from("message"),
                        set: Some(format!("messages-{}", set_idx)),
                        label: Some(format!("message-{}", label_idx)),
                        data: json!({
                            "text": format!("Message {}, version {}", label_idx, event_idx)
                        }),
                        is_persistent: true,
                    },
                ).await;

                if response.properties().status() != ResponseStatus::CREATED {
                    panic!("Failed to create event");
                }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    info!("Requesting state.");

    let sets = (0..sets_count)
        .map(|i| format!("messages-{}", i))
        .collect::<Vec<String>>();

    let payload = StateReadRequest {
        room_id,
        kind: String::from("message"),
        sets,
        occurred_at: Utc::now().timestamp(),
        limit: labels_count,
    };

    let start = Utc::now();
    let response = agent.request::<StateReadRequest, StateReadResponse>("state.read", payload).await;
    let request_duration = Utc::now() - start;

    if response.properties().status() != ResponseStatus::OK {
        panic!("Failed to read state");
    }

    println!(
        "Request duration: {} ms",
        request_duration.num_milliseconds()
    );
}
