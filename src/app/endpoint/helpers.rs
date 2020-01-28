use chrono::{DateTime, Utc};
use serde::ser::Serialize;
use svc_agent::mqtt::{
    IncomingRequestProperties, IntoPublishableDump, OutgoingEvent, OutgoingEventProperties,
    OutgoingResponse, ResponseStatus, ShortTermTimingProperties,
};
use svc_error::Error as SvcError;

use crate::app::API_VERSION;

pub(crate) fn build_response(
    status: ResponseStatus,
    payload: impl Serialize + 'static,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> Box<dyn IntoPublishableDump> {
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let props = reqp.to_response(status, timing);
    let resp = OutgoingResponse::unicast(payload, props, reqp, API_VERSION);
    Box::new(resp) as Box<dyn IntoPublishableDump>
}

pub(crate) fn build_error_response(
    status: ResponseStatus,
    title: &str,
    detail: &str,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> Box<dyn IntoPublishableDump> {
    let error = SvcError::builder()
        .status(status)
        .kind(reqp.method(), title)
        .detail(detail)
        .build();

    build_response(status, error, reqp, start_timestamp)
}

pub(crate) fn build_notification(
    label: &'static str,
    path: &str,
    payload: impl Serialize + 'static,
    reqp: &IncomingRequestProperties,
    start_timestamp: DateTime<Utc>,
) -> Box<dyn IntoPublishableDump> {
    let timing = ShortTermTimingProperties::until_now(start_timestamp);
    let mut props = OutgoingEventProperties::new(label, timing);
    props.set_tracking(reqp.tracking().to_owned());
    Box::new(OutgoingEvent::broadcast(payload, props, path))
}
