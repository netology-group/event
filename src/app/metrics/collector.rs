use std::sync::atomic::Ordering;

use anyhow::{anyhow, Context as AnyhowContext};
use chrono::{DateTime, Utc};

use crate::app::metrics::{Metric, MetricValue, ProfilerKeys, Tags};
use crate::app::Context;

pub(crate) struct Collector<'a> {
    context: &'a dyn Context,
    duration: u64,
}

impl<'a> Collector<'a> {
    pub(crate) fn new(context: &'a dyn Context, duration: u64) -> Self {
        Self { context, duration }
    }

    pub(crate) fn get(&self) -> anyhow::Result<Vec<crate::app::metrics::Metric>> {
        let now = Utc::now();
        let mut metrics = vec![];

        append_mqtt_stats(&mut metrics, self.context, now, self.duration)?;
        append_internal_stats(&mut metrics, self.context, now);
        append_redis_pool_metrics(&mut metrics, self.context, now);

        append_profiler_stats(&mut metrics, self.context, now, self.duration)?;

        if let Some(counter) = self.context.running_requests() {
            let tags = Tags::build_internal_tags(crate::APP_VERSION, &self.context.agent_id());
            metrics.push(Metric::RunningRequests(MetricValue::new(
                counter.load(Ordering::SeqCst),
                now,
                tags,
            )));
        }

        Ok(metrics)
    }
}

fn append_mqtt_stats(
    metrics: &mut Vec<Metric>,
    context: &dyn Context,
    now: DateTime<Utc>,
    duration: u64,
) -> anyhow::Result<()> {
    if let Some(qc) = context.queue_counter() {
        let stats = qc
            .get_stats(duration)
            .map_err(|err| anyhow!(err).context("Failed to get stats"))?;

        stats.into_iter().for_each(|(tags, value)| {
            let tags = Tags::build_queues_tags(crate::APP_VERSION, context.agent_id(), tags);

            let m = [
                Metric::IncomingQueueRequests(MetricValue::new(
                    value.incoming_requests,
                    now,
                    tags.clone(),
                )),
                Metric::IncomingQueueResponses(MetricValue::new(
                    value.incoming_responses,
                    now,
                    tags.clone(),
                )),
                Metric::IncomingQueueEvents(MetricValue::new(
                    value.incoming_events,
                    now,
                    tags.clone(),
                )),
                Metric::OutgoingQueueRequests(MetricValue::new(
                    value.outgoing_requests,
                    now,
                    tags.clone(),
                )),
                Metric::OutgoingQueueResponses(MetricValue::new(
                    value.outgoing_responses,
                    now,
                    tags.clone(),
                )),
                Metric::OutgoingQueueEvents(MetricValue::new(value.outgoing_events, now, tags)),
            ];

            metrics.extend_from_slice(&m);
        });
    }

    Ok(())
}

fn append_internal_stats(metrics: &mut Vec<Metric>, context: &dyn Context, now: DateTime<Utc>) {
    let tags = Tags::build_internal_tags(crate::APP_VERSION, context.agent_id());

    metrics.extend_from_slice(&[
        Metric::DbConnections(MetricValue::new(
            context.db().size() as u64,
            now,
            tags.clone(),
        )),
        Metric::IdleDbConnections(MetricValue::new(
            context.db().num_idle() as u64,
            now,
            tags.clone(),
        )),
        Metric::RoDbConnections(MetricValue::new(
            context.ro_db().size() as u64,
            now,
            tags.clone(),
        )),
        Metric::IdleRoDbConnections(MetricValue::new(
            context.ro_db().num_idle() as u64,
            now,
            tags,
        )),
    ])
}

fn append_redis_pool_metrics(metrics: &mut Vec<Metric>, context: &dyn Context, now: DateTime<Utc>) {
    if let Some(pool) = context.redis_pool() {
        let state = pool.state();
        let tags = Tags::build_internal_tags(crate::APP_VERSION, context.agent_id());

        metrics.extend_from_slice(&[
            Metric::RedisConnections(MetricValue::new(
                state.connections as u64,
                now,
                tags.clone(),
            )),
            Metric::IdleRedisConnections(MetricValue::new(
                state.idle_connections as u64,
                now,
                tags,
            )),
        ]);
    }
}

fn append_profiler_stats(
    metrics: &mut Vec<Metric>,
    context: &dyn Context,
    now: DateTime<Utc>,
    duration: u64,
) -> anyhow::Result<()> {
    let profiler_report = context
        .profiler()
        .flush(duration)
        .context("Failed to flush profiler")?;

    for ((key, method), entry_report) in profiler_report {
        let tags = Tags::build_queries_tags(crate::APP_VERSION, context.agent_id(), key, method);
        let metric_value_p95 = MetricValue::new(entry_report.p95 as u64, now, tags.clone());
        let metric_value_p99 = MetricValue::new(entry_report.p99 as u64, now, tags.clone());
        let metric_value_max = MetricValue::new(entry_report.max as u64, now, tags.clone());

        match key {
            ProfilerKeys::AdjustmentInsertQuery => {
                metrics.push(Metric::AdjustmentInsertQueryP95(metric_value_p95));
                metrics.push(Metric::AdjustmentInsertQueryP99(metric_value_p99));
                metrics.push(Metric::AdjustmentInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::AgentDeleteQuery => {
                metrics.push(Metric::AgentDeleteQueryP95(metric_value_p95));
                metrics.push(Metric::AgentDeleteQueryP99(metric_value_p99));
                metrics.push(Metric::AgentDeleteQueryMax(metric_value_max));
            }
            ProfilerKeys::AgentInsertQuery => {
                metrics.push(Metric::AgentInsertQueryP95(metric_value_p95));
                metrics.push(Metric::AgentInsertQueryP99(metric_value_p99));
                metrics.push(Metric::AgentInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::AgentListQuery => {
                metrics.push(Metric::AgentListQueryP95(metric_value_p95));
                metrics.push(Metric::AgentListQueryP99(metric_value_p99));
                metrics.push(Metric::AgentListQueryMax(metric_value_max));
            }
            ProfilerKeys::AgentUpdateQuery => {
                metrics.push(Metric::AgentUpdateQueryP95(metric_value_p95));
                metrics.push(Metric::AgentUpdateQueryP99(metric_value_p99));
                metrics.push(Metric::AgentUpdateQueryMax(metric_value_max));
            }
            ProfilerKeys::ChangeDeleteQuery => {
                metrics.push(Metric::ChangeDeleteQueryP95(metric_value_p95));
                metrics.push(Metric::ChangeDeleteQueryP99(metric_value_p99));
                metrics.push(Metric::ChangeDeleteQueryMax(metric_value_max));
            }
            ProfilerKeys::ChangeFindWithRoomQuery => {
                metrics.push(Metric::ChangeFindWithRoomQueryP95(metric_value_p95));
                metrics.push(Metric::ChangeFindWithRoomQueryP99(metric_value_p99));
                metrics.push(Metric::ChangeFindWithRoomQueryMax(metric_value_max));
            }
            ProfilerKeys::ChangeInsertQuery => {
                metrics.push(Metric::ChangeInsertQueryP95(metric_value_p95));
                metrics.push(Metric::ChangeInsertQueryP99(metric_value_p99));
                metrics.push(Metric::ChangeInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::ChangeListQuery => {
                metrics.push(Metric::ChangeListQueryP95(metric_value_p95));
                metrics.push(Metric::ChangeListQueryP99(metric_value_p99));
                metrics.push(Metric::ChangeListQueryMax(metric_value_max));
            }
            ProfilerKeys::EditionCloneEventsQuery => {
                metrics.push(Metric::EditionCloneEventsQueryP95(metric_value_p95));
                metrics.push(Metric::EditionCloneEventsQueryP99(metric_value_p99));
                metrics.push(Metric::EditionCloneEventsQueryMax(metric_value_max));
            }
            ProfilerKeys::EditionCommitTxnCommit => {
                metrics.push(Metric::EditionCommitTxnCommitP95(metric_value_p95));
                metrics.push(Metric::EditionCommitTxnCommitP99(metric_value_p99));
                metrics.push(Metric::EditionCommitTxnCommitMax(metric_value_max));
            }
            ProfilerKeys::EditionDeleteQuery => {
                metrics.push(Metric::EditionDeleteQueryP95(metric_value_p95));
                metrics.push(Metric::EditionDeleteQueryP99(metric_value_p99));
                metrics.push(Metric::EditionDeleteQueryMax(metric_value_max));
            }
            ProfilerKeys::EditionFindWithRoomQuery => {
                metrics.push(Metric::EditionFindWithRoomQueryP95(metric_value_p95));
                metrics.push(Metric::EditionFindWithRoomQueryP99(metric_value_p99));
                metrics.push(Metric::EditionFindWithRoomQueryMax(metric_value_max));
            }
            ProfilerKeys::EditionInsertQuery => {
                metrics.push(Metric::EditionInsertQueryP95(metric_value_p95));
                metrics.push(Metric::EditionInsertQueryP99(metric_value_p99));
                metrics.push(Metric::EditionInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::EditionListQuery => {
                metrics.push(Metric::EditionListQueryP95(metric_value_p95));
                metrics.push(Metric::EditionListQueryP99(metric_value_p99));
                metrics.push(Metric::EditionListQueryMax(metric_value_max));
            }
            ProfilerKeys::EventDeleteQuery => {
                metrics.push(Metric::EventDeleteQueryP95(metric_value_p95));
                metrics.push(Metric::EventDeleteQueryP99(metric_value_p99));
                metrics.push(Metric::EventDeleteQueryMax(metric_value_max));
            }
            ProfilerKeys::EventInsertQuery => {
                metrics.push(Metric::EventInsertQueryP95(metric_value_p95));
                metrics.push(Metric::EventInsertQueryP99(metric_value_p99));
                metrics.push(Metric::EventInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::EventListQuery => {
                metrics.push(Metric::EventListQueryP95(metric_value_p95));
                metrics.push(Metric::EventListQueryP99(metric_value_p99));
                metrics.push(Metric::EventListQueryMax(metric_value_max));
            }
            ProfilerKeys::EventOriginalEventQuery => {
                metrics.push(Metric::EventOriginalQueryP95(metric_value_p95));
                metrics.push(Metric::EventOriginalQueryP99(metric_value_p99));
                metrics.push(Metric::EventOriginalQueryMax(metric_value_max));
            }
            ProfilerKeys::RoomAdjustCloneEventsQuery => {
                metrics.push(Metric::RoomAdjustCloneEventsQueryP95(metric_value_p95));
                metrics.push(Metric::RoomAdjustCloneEventsQueryP99(metric_value_p99));
                metrics.push(Metric::RoomAdjustCloneEventsQueryMax(metric_value_max));
            }
            ProfilerKeys::RoomFindQuery => {
                metrics.push(Metric::RoomFindQueryP95(metric_value_p95));
                metrics.push(Metric::RoomFindQueryP99(metric_value_p99));
                metrics.push(Metric::RoomFindQueryMax(metric_value_max));
            }
            ProfilerKeys::RoomInsertQuery => {
                metrics.push(Metric::RoomInsertQueryP95(metric_value_p95));
                metrics.push(Metric::RoomInsertQueryP99(metric_value_p99));
                metrics.push(Metric::RoomInsertQueryMax(metric_value_max));
            }
            ProfilerKeys::RoomUpdateQuery => {
                metrics.push(Metric::RoomUpdateQueryP95(metric_value_p95));
                metrics.push(Metric::RoomUpdateQueryP99(metric_value_p99));
                metrics.push(Metric::RoomUpdateQueryMax(metric_value_max));
            }
            ProfilerKeys::StateTotalCountQuery => {
                metrics.push(Metric::StateTotalCountQueryP95(metric_value_p95));
                metrics.push(Metric::StateTotalCountQueryP99(metric_value_p99));
                metrics.push(Metric::StateTotalCountQueryMax(metric_value_max));
            }
            ProfilerKeys::StateQuery => {
                metrics.push(Metric::StateQueryP95(metric_value_p95));
                metrics.push(Metric::StateQueryP99(metric_value_p99));
                metrics.push(Metric::StateQueryMax(metric_value_max));
            }
        }
    }
    Ok(())
}