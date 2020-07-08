use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use chrono::Utc;
use futures::StreamExt;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_derive::Deserialize;
use svc_agent::{
    mqtt::{
        Agent, AgentBuilder, AgentConfig, AgentNotification, ConnectionMode, IncomingEvent,
        IncomingMessage, IncomingResponse, OutgoingMessage, OutgoingRequest,
        OutgoingRequestProperties, QoS, ShortTermTimingProperties, SubscriptionTopic,
    },
    request::Dispatcher as RequestDispatcher,
    AccountId, AgentId, Subscription,
};

///////////////////////////////////////////////////////////////////////////////

const API_VERSION: &'static str = "v1";
const CORRELATION_DATA_LENGTH: usize = 16;
const IGNORE: &'static str = "ignore";

#[derive(Deserialize)]
struct IgnoreResponse {}

pub struct TestAgent {
    id: AgentId,
    agent: Agent,
    request_dispatcher: Arc<RequestDispatcher>,
    event_dispatcher: Arc<EventDispatcher>,
    service_account_id: AccountId,
    inbox_topic: String,
}

impl TestAgent {
    pub fn start(
        config: &AgentConfig,
        id: AgentId,
        service_account_id: &AccountId,
    ) -> Self {
        // Start agent.
        let (mut agent, rx) = AgentBuilder::new(id.clone(), API_VERSION)
            .connection_mode(ConnectionMode::Default)
            .start(&config)
            .expect("Failed to start agent");

        // Subscribe to the service responses.
        let subscription = Subscription::unicast_responses_from(service_account_id);

        agent
            .subscribe(&subscription, QoS::AtLeastOnce, None)
            .expect("Error subscribing to unicast responses");

        let inbox_topic = subscription
            .subscription_topic(&id, API_VERSION)
            .expect("Failed to build response topic");

        // Run dispatchers.
        let request_dispatcher = Arc::new(RequestDispatcher::new(&agent));
        let request_dispatcher_clone = request_dispatcher.clone();

        let event_dispatcher = Arc::new(EventDispatcher::new());
        let event_dispatcher_clone = event_dispatcher.clone();

        let (mq_tx, mut mq_rx) = futures_channel::mpsc::unbounded::<AgentNotification>();

        std::thread::Builder::new()
            .name("test-agent-notifications-loop".to_owned())
            .spawn(move || {
                for message in rx {
                    if mq_tx.unbounded_send(message).is_err() {
                        eprintln!("Error sending message to the internal channel");
                    }
                }
            })
        .expect("Failed to start event notifications loop");

        async_std::task::spawn(async move {
            loop {
                let q = mq_rx.next().await;
                if let Some(AgentNotification::Message(Ok(message), _)) = q {
                    match message {
                        IncomingMessage::Request(_) => (),
                        IncomingMessage::Response(resp) => {
                            if resp.properties().correlation_data() != IGNORE {
                                let request_dispatcher_clone_ = request_dispatcher_clone.clone();

                                async_std::task::spawn(async move {
                                    let json = serde_json::from_str(resp.payload())
                                        .expect("Failed to parse response");

                                    let respp = resp.properties().to_owned();

                                    request_dispatcher_clone_
                                        .response(IncomingResponse::new(json, respp))
                                        .await
                                        .expect("Failed to dispatch response");
                                });
                            }
                        }
                        ev @ IncomingMessage::Event(_) => event_dispatcher_clone.dispatch(ev),
                    };
                }
            }
        });

        Self {
            id,
            agent,
            request_dispatcher,
            event_dispatcher,
            service_account_id: service_account_id.to_owned(),
            inbox_topic,
        }
    }

    pub fn id(&self) -> &AgentId {
        &self.id
    }

    /// Publish a request and wait for the response.
    pub async fn request<Req, Resp>(&self, method: &str, payload: Req) -> IncomingResponse<Resp>
    where
        Req: 'static + Send + Serialize,
        Resp: 'static + Send + DeserializeOwned,
    {
        let correlation_data = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(CORRELATION_DATA_LENGTH)
            .collect::<String>();

        let request = match self.build_request(method, &correlation_data, payload) {
            OutgoingMessage::Request(req) => req,
            _ => panic!("Expected outgoing request"),
        };

        let (resp_tx, resp_rx) = futures::channel::oneshot::channel::<IncomingResponse<Resp>>();
        let dispatcher = self.request_dispatcher.clone();

        async_std::task::spawn(async move {
            let response = dispatcher
                .request::<Req, Resp>(request)
                .await
                .expect("Failed to dispatch request");

            resp_tx
                .send(response)
                .map_err(|_e| "")
                .expect("Failed to notify about received response");
        });

        resp_rx
            .await
            .expect("Failed to await response")
    }

    /// Publish a request but don't wait for the response.
    pub fn request_nowait<Req>(&self, method: &str, payload: Req)
    where
        Req: 'static + Send + Serialize,
    {
        self.agent
            .clone()
            .publish(self.build_request(method, IGNORE, payload))
            .expect("Failed to publish request");
    }

    fn build_request<P: Serialize>(
        &self,
        method: &str,
        correlation_data: &str,
        payload: P,
    ) -> OutgoingMessage<P> {
        let mut reqp = OutgoingRequestProperties::new(
            method,
            &self.inbox_topic,
            correlation_data,
            ShortTermTimingProperties::new(Utc::now()),
        );

        reqp.set_local_timestamp(Utc::now());
        OutgoingRequest::multicast(payload, reqp, &self.service_account_id)
    }

    /// Blocks until receiving an event with `label` that matches `predicate`.
    pub fn wait_for_event<P, F>(&self, label: &str, predicate: F)
    where
        P: DeserializeOwned,
        F: Fn(&IncomingEvent<P>) -> bool,
    {
        self.event_dispatcher.wait(label, |evt| {
            let payload = serde_json::from_str::<P>(evt.payload()).expect("Failed to parse");
            predicate(&IncomingEvent::new(payload, evt.properties().to_owned()))
        });
    }
}

///////////////////////////////////////////////////////////////////////////////

struct EventDispatcher {
    tx: Arc<Mutex<mpsc::Sender<IncomingMessage<String>>>>,
    rx: Arc<Mutex<mpsc::Receiver<IncomingMessage<String>>>>,
    is_waiting: AtomicBool,
}

impl EventDispatcher {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<IncomingMessage<String>>();

        Self {
            tx: Arc::new(Mutex::new(tx)),
            rx: Arc::new(Mutex::new(rx)),
            is_waiting: AtomicBool::new(false),
        }
    }

    fn wait<F>(&self, label: &str, predicate: F) -> IncomingEvent<String>
    where
        F: Fn(&IncomingEvent<String>) -> bool,
    {
        let rx = self
            .rx
            .lock()
            .expect("Failed to obtain channel receiver lock");

        // Start receiving messages.
        self.is_waiting.store(true, Ordering::SeqCst);

        // Receive messages until facing an event with the expected label.
        while let Ok(IncomingMessage::Event(event)) = rx.recv() {
            // Skip messages with unexpected label.
            if event.properties().label() != Some(label) {
                continue;
            }

            // Run caller-defined predicate to check whether it's the expected event or
            // we should wait for another one.
            if !predicate(&event) {
                continue;
            }

            // Stop receiving messages.
            self.is_waiting.store(false, Ordering::SeqCst);

            // Read the rest of the queue to avoid memory leak.
            while let Ok(_) = rx.try_recv() {}

            // Return the parsed event.
            return event;
        }

        panic!("Failed to wait for `{}` event. Channel closed.", label);
    }

    fn dispatch(&self, message: IncomingMessage<String>) {
        // Push message to the channel only if the other thread is waiting to avoid memory leak.
        if self.is_waiting.load(Ordering::SeqCst) {
            let tx = self
                .tx
                .lock()
                .expect("Failed to obtain channel sender lock");

            tx.send(message).expect("Failed to dispatch event");
        }
    }
}
