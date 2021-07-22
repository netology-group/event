use std::cmp;
use std::ops::Bound;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use sqlx::postgres::{PgConnection, PgPool as Db};

use crate::app::metrics::ProfilerKeys;
use crate::db::adjustment::{InsertQuery as AdjustmentInsertQuery, Segments};
use crate::db::event::{
    DeleteQuery as EventDeleteQuery, ListQuery as EventListQuery, Object as Event,
};
use crate::db::room::{InsertQuery as RoomInsertQuery, Object as Room};
use crate::db::room_time::RoomTimeBound;
use crate::profiler::Profiler;

pub(crate) const NANOSECONDS_IN_MILLISECOND: i64 = 1_000_000;

////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn call(
    db: &Db,
    profiler: &Profiler<(ProfilerKeys, Option<String>)>,
    real_time_room: &Room,
    started_at: DateTime<Utc>,
    segments: &Segments,
    offset: i64,
) -> Result<(Room, Room, Segments)> {
    info!(
        crate::LOG,
        "Room adjustment task started for room_id = '{}'",
        real_time_room.id()
    );
    let start_timestamp = Utc::now();

    // Parse segments.
    let bounded_offset_tuples: Vec<(Bound<i64>, Bound<i64>)> = segments.to_owned().into();
    let mut parsed_segments = Vec::with_capacity(bounded_offset_tuples.len());

    for segment in bounded_offset_tuples {
        match segment {
            (Bound::Included(start), Bound::Excluded(stop)) => parsed_segments.push((start, stop)),
            segment => bail!("Invalid segment: {:?}", segment),
        }
    }

    // Create adjustment.
    let mut conn = db
        .acquire()
        .await
        .context("Failed to acquire db connection")?;

    let time = real_time_room
        .time()
        .map_err(|e| anyhow!(e))
        .context("Invalid room time")?;
    let real_time_room_new_time = if *time.end() == RoomTimeBound::Unbounded {
        let new_time = match time.update((
            Bound::Included(*time.start()),
            Bound::Excluded(start_timestamp),
        )) {
            Some(new_time) => new_time,
            None => {
                bail!(format!(
                    "Failed to update room time bound, invalid room time, room_id = '{}'",
                    real_time_room.id(),
                ))
            }
        };

        let query = crate::db::room::UpdateQuery::new(real_time_room.id())
            .time(Some(new_time.clone().into()));

        profiler
            .measure(
                (ProfilerKeys::RoomUpdateQuery, Some("room.adjust".into())),
                query.execute(&mut conn),
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to update room time bound, room_id = '{}'",
                    real_time_room.id(),
                )
            })?;
        new_time
    } else {
        time
    };

    let query =
        AdjustmentInsertQuery::new(real_time_room.id(), started_at, segments.to_owned(), offset);

    profiler
        .measure(
            (
                ProfilerKeys::AdjustmentInsertQuery,
                Some("room.adjust".into()),
            ),
            query.execute(&mut conn),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to insert adjustment, room_id = '{}'",
                real_time_room.id(),
            )
        })?;

    ///////////////////////////////////////////////////////////////////////////

    // Get room opening time and duration.
    let (room_opening, room_duration) = match real_time_room_new_time.end() {
        RoomTimeBound::Excluded(stop) => {
            let start = real_time_room_new_time.start();
            (*start, stop.signed_duration_since(*start))
        }
        _ => bail!("invalid duration for room = '{}'", real_time_room.id()),
    };

    /*let (room_opening, room_duration) = match real_time_room.time() {
        Ok(t) => match t.end() {
            RoomTimeBound::Excluded(stop) => (*t.start(), stop.signed_duration_since(*t.start())),
            _ => bail!("invalid duration for room = '{}'", real_time_room.id()),
        },
        _ => bail!("invalid duration for room = '{}'", real_time_room.id()),
    };*/

    // Calculate RTC offset as the difference between event room opening and RTC start.
    let rtc_offset = (started_at - room_opening).num_milliseconds();

    // Convert segments to nanoseconds.
    let nano_segments = parsed_segments
        .iter()
        .map(|(start, stop)| {
            let nano_start = (start + rtc_offset) * NANOSECONDS_IN_MILLISECOND;
            let nano_stop = (stop + rtc_offset) * NANOSECONDS_IN_MILLISECOND;
            (nano_start, nano_stop)
        })
        .collect::<Vec<(i64, i64)>>();

    // Invert segments to gaps.
    let segment_gaps = invert_segments(&nano_segments, room_duration)?;

    // Calculate total duration of initial segments.
    let total_segments_millis = parsed_segments
        .into_iter()
        .fold(0, |acc, (start, stop)| acc + (stop - start));

    let total_segments_duration = Duration::milliseconds(total_segments_millis);

    // Create original room with events shifted according to segments.
    let original_room = create_room(
        &mut conn,
        profiler,
        &real_time_room,
        started_at,
        total_segments_duration,
    )
    .await?;

    clone_events(
        &mut conn,
        profiler,
        &original_room,
        &segment_gaps,
        (offset - rtc_offset) * NANOSECONDS_IN_MILLISECOND,
    )
    .await?;

    ///////////////////////////////////////////////////////////////////////////

    // Fetch shifted cut events and transform them to gaps.
    let query = EventListQuery::new()
        .room_id(original_room.id())
        .kind("stream".to_string());

    let cut_events = profiler
        .measure(
            (ProfilerKeys::EventListQuery, Some("room.adjust".into())),
            query.execute(&mut conn),
        )
        .await
        .with_context(|| {
            format!(
                "failed to fetch cut events for room_id = '{}'",
                original_room.id()
            )
        })?;

    let cut_gaps = cut_events_to_gaps(&cut_events)?;

    // Create modified room with events shifted again according to cut events this time.
    let modified_room = create_room(
        &mut conn,
        profiler,
        &original_room,
        started_at,
        total_segments_duration,
    )
    .await?;
    clone_events(&mut conn, profiler, &modified_room, &cut_gaps, 0).await?;

    // Delete cut events from the modified room.
    let query = EventDeleteQuery::new(modified_room.id(), "stream");

    profiler
        .measure(
            (ProfilerKeys::EventDeleteQuery, Some("room.adjust".into())),
            query.execute(&mut conn),
        )
        .await
        .with_context(|| {
            format!(
                "failed to delete cut events for room_id = '{}'",
                modified_room.id()
            )
        })?;

    ///////////////////////////////////////////////////////////////////////////

    // Calculate modified segments by inverting cut gaps limited by total initial segments duration.
    let modified_segments = invert_segments(&cut_gaps, total_segments_duration)?
        .into_iter()
        .map(|(start, stop)| {
            (
                Bound::Included(cmp::max(start / NANOSECONDS_IN_MILLISECOND, 0)),
                Bound::Excluded(stop / NANOSECONDS_IN_MILLISECOND),
            )
        })
        .collect::<Vec<(Bound<i64>, Bound<i64>)>>();

    ///////////////////////////////////////////////////////////////////////////

    // Done.
    info!(
        crate::LOG,
        "Room adjustment task successfully finished for room_id = '{}', duration = {} ms",
        real_time_room.id(),
        (Utc::now() - start_timestamp).num_milliseconds()
    );

    Ok((
        original_room,
        modified_room,
        Segments::from(modified_segments),
    ))
}

/// Creates a derived room from the source room.
async fn create_room(
    conn: &mut PgConnection,
    profiler: &Profiler<(ProfilerKeys, Option<String>)>,
    source_room: &Room,
    started_at: DateTime<Utc>,
    room_duration: Duration,
) -> Result<Room> {
    let time = (
        Bound::Included(started_at),
        Bound::Excluded(started_at + room_duration),
    );
    let mut query = RoomInsertQuery::new(&source_room.audience(), time.into());
    query = query.source_room_id(source_room.id());

    if let Some(tags) = source_room.tags() {
        query = query.tags(tags.to_owned());
    }

    profiler
        .measure(
            (ProfilerKeys::RoomInsertQuery, Some("room.adjust".into())),
            query.execute(conn),
        )
        .await
        .context("failed to insert room")
}

/// Clones events from the source room of the `room` with shifting them according to `gaps` and
/// adding `offset` (both in nanoseconds).
async fn clone_events(
    conn: &mut PgConnection,
    profiler: &Profiler<(ProfilerKeys, Option<String>)>,
    room: &Room,
    gaps: &[(i64, i64)],
    offset: i64,
) -> Result<()> {
    let source_room_id = match room.source_room_id() {
        Some(id) => id,
        None => bail!("room = '{}' is not derived from another room", room.id()),
    };

    let mut starts = Vec::with_capacity(gaps.len());
    let mut stops = Vec::with_capacity(gaps.len());

    for (start, stop) in gaps {
        starts.push(*start);
        stops.push(*stop);
    }

    let query = sqlx::query!(
        "
        WITH
            gap_starts AS (
                SELECT start, ROW_NUMBER() OVER () AS row_number
                FROM UNNEST($1::BIGINT[]) AS start
            ),
            gap_stops AS (
                SELECT stop, ROW_NUMBER() OVER () AS row_number
                FROM UNNEST($2::BIGINT[]) AS stop
            ),
            gaps AS (
                SELECT start, stop
                FROM gap_starts, gap_stops
                WHERE gap_stops.row_number = gap_starts.row_number
            )
        INSERT INTO event (id, room_id, kind, set, label, data, occurred_at, created_by, created_at)
        SELECT
            id,
            room_id,
            kind,
            set,
            label,
            data,
            -- Monotonization
            occurred_at + ROW_NUMBER() OVER (PARTITION BY occurred_at ORDER BY created_at) - 1,
            created_by,
            created_at
        FROM (
            SELECT
                gen_random_uuid() AS id,
                $3::UUID AS room_id,
                kind,
                set,
                label,
                data,
                CASE occurred_at <= (SELECT stop FROM gaps WHERE start = 0)
                WHEN TRUE THEN (SELECT stop FROM gaps WHERE start = 0)
                ELSE occurred_at - (
                    SELECT COALESCE(SUM(LEAST(stop, occurred_at) - start), 0)
                    FROM gaps
                    WHERE start < occurred_at
                    AND   start > 0
                )
                END + $4 AS occurred_at,
                created_by,
                created_at
            FROM event
            WHERE room_id = $5
            AND   deleted_at IS NULL
        ) AS sub
        ",
        starts.as_slice(),
        stops.as_slice(),
        room.id(),
        sqlx::types::BigDecimal::from(offset),
        source_room_id,
    );

    profiler
        .measure(
            (
                ProfilerKeys::RoomAdjustCloneEventsQuery,
                Some("room.adjust".into()),
            ),
            query.execute(conn),
        )
        .await
        .map(|_| ())
        .with_context(|| {
            format!(
                "failed to shift clone events from to room = '{}'",
                room.id()
            )
        })
}

/// Turns `segments` into gaps.
pub(crate) fn invert_segments(
    segments: &[(i64, i64)],
    room_duration: Duration,
) -> Result<Vec<(i64, i64)>> {
    if segments.is_empty() {
        let total_nanos = room_duration.num_nanoseconds().unwrap_or(std::i64::MAX);
        return Ok(vec![(0, total_nanos)]);
    }

    let mut gaps = Vec::with_capacity(segments.len() + 2);

    // A possible gap before the first segment.
    if let Some((first_segment_start, _)) = segments.first() {
        if *first_segment_start > 0 {
            gaps.push((0, *first_segment_start));
        }
    }

    // Gaps between segments.
    for ((_, segment_stop), (next_segment_start, _)) in segments.iter().zip(&segments[1..]) {
        gaps.push((*segment_stop, *next_segment_start));
    }

    // A possible gap after the last segment.
    if let Some((_, last_segment_stop)) = segments.last() {
        let room_duration_nanos = room_duration.num_nanoseconds().unwrap_or(std::i64::MAX);

        if *last_segment_stop < room_duration_nanos {
            gaps.push((*last_segment_stop, room_duration_nanos));
        }
    }

    Ok(gaps)
}

#[derive(Clone, Copy, Debug)]
enum CutEventsToGapsState {
    Started(i64),
    Stopped,
}

/// Transforms cut-start/stop events ordered list to gaps list with a simple FSM.
pub(crate) fn cut_events_to_gaps(cut_events: &[Event]) -> Result<Vec<(i64, i64)>> {
    let mut gaps = Vec::with_capacity(cut_events.len());
    let mut state: CutEventsToGapsState = CutEventsToGapsState::Stopped;

    for event in cut_events {
        let command = event.data().get("cut").and_then(|v| v.as_str());

        match (command, state) {
            (Some("start"), CutEventsToGapsState::Stopped) => {
                state = CutEventsToGapsState::Started(event.occurred_at());
            }
            (Some("stop"), CutEventsToGapsState::Started(start)) => {
                gaps.push((start, event.occurred_at()));
                state = CutEventsToGapsState::Stopped;
            }
            _ => bail!(
                "invalid cut event, id = '{}', command = {:?}, state = {:?}",
                event.id(),
                command,
                state
            ),
        }
    }

    Ok(gaps)
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use chrono::{DateTime, Duration, NaiveDateTime, Utc};
    use serde_json::{json, Value as JsonValue};
    use sqlx::postgres::PgConnection;
    use svc_agent::{AccountId, AgentId};

    use crate::db::adjustment::Segments;
    use crate::db::event::{
        InsertQuery as EventInsertQuery, ListQuery as EventListQuery, Object as Event,
    };
    use crate::db::room::{InsertQuery as RoomInsertQuery, Object as Room, Time as RoomTime};
    use crate::profiler::Profiler;
    use crate::test_helpers::db::TestDb;
    use crate::{app::metrics::ProfilerKeys, db::room_time::RoomTimeBound};

    const AUDIENCE: &str = "dev.svc.example.org";

    #[test]
    fn adjust_room() {
        async_std::task::block_on(async {
            let profiler = Profiler::<(ProfilerKeys, Option<String>)>::start();
            let db = TestDb::new().await;
            let mut conn = db.get_conn().await;

            // Create room.
            let opened_at = DateTime::from_utc(NaiveDateTime::from_timestamp(1582002673, 0), Utc);
            let closed_at = opened_at + Duration::seconds(50);
            let time = RoomTime::from((Bound::Included(opened_at), Bound::Excluded(closed_at)));

            let room = RoomInsertQuery::new(AUDIENCE, time)
                .execute(&mut conn)
                .await
                .expect("Failed to insert room");

            create_events(&mut conn, &room).await;

            drop(conn);

            // Video segments.
            let segments = Segments::from(vec![
                (Bound::Included(0), Bound::Excluded(6500)),
                (Bound::Included(8500), Bound::Excluded(11500)),
            ]);
            let duration = Duration::milliseconds(6500 + 11500 - 8500);
            // RTC started 10 seconds after the room opened and preroll is 3 seconds long.
            let started_at = opened_at + Duration::seconds(10);

            // Call room adjustment.
            let (original_room, modified_room, modified_segments) = super::call(
                &db.connection_pool(),
                &profiler,
                &room,
                started_at,
                &segments,
                3000 as i64,
            )
            .await
            .expect("Room adjustment failed");

            // Assert original room.
            assert_eq!(original_room.source_room_id(), Some(room.id()));
            assert_eq!(original_room.audience(), room.audience());
            assert_eq!(original_room.time().map(|t| *t.start()), Ok(started_at));
            assert_eq!(
                original_room.time().map(|t| t.end().to_owned()),
                Ok(RoomTimeBound::Excluded(started_at + duration))
            );
            assert_eq!(original_room.tags(), room.tags());

            // Assert original room events.
            let mut conn = db.get_conn().await;
            assert_events_original_room(&mut conn, &original_room).await;

            // Assert modified room.
            assert_eq!(modified_room.source_room_id(), Some(original_room.id()));
            assert_eq!(modified_room.audience(), original_room.audience());
            assert_eq!(modified_room.time(), original_room.time());
            assert_eq!(modified_room.tags(), original_room.tags());

            assert_events_modified_room(&mut conn, &modified_room).await;

            // Assert modified segments.
            let segments: Vec<(Bound<i64>, Bound<i64>)> = modified_segments.into();

            assert_eq!(
                segments,
                vec![
                    (Bound::Included(0), Bound::Excluded(6000)),
                    (Bound::Included(8000), Bound::Excluded(9500)),
                ]
            )
        });
    }

    #[test]
    fn adjust_room_unbounbded() {
        async_std::task::block_on(async {
            let profiler = Profiler::<(ProfilerKeys, Option<String>)>::start();
            let db = TestDb::new().await;
            let mut conn = db.get_conn().await;

            // Create room.
            let opened_at = DateTime::from_utc(NaiveDateTime::from_timestamp(1582002673, 0), Utc);
            let time = RoomTime::from((Bound::Included(opened_at), Bound::Unbounded));

            let room = RoomInsertQuery::new(AUDIENCE, time)
                .execute(&mut conn)
                .await
                .expect("Failed to insert room");

            create_events(&mut conn, &room).await;

            drop(conn);

            // Video segments.
            let segments = Segments::from(vec![
                (Bound::Included(0), Bound::Excluded(6500)),
                (Bound::Included(8500), Bound::Excluded(11500)),
            ]);
            let duration = Duration::milliseconds(6500 + 11500 - 8500);

            // RTC started 10 seconds after the room opened and preroll is 3 seconds long.
            let started_at = opened_at + Duration::seconds(10);

            // Call room adjustment.
            let (original_room, modified_room, modified_segments) = super::call(
                &db.connection_pool(),
                &profiler,
                &room,
                started_at,
                &segments,
                3000 as i64,
            )
            .await
            .expect("Room adjustment failed");

            // Assert original room.
            assert_eq!(original_room.source_room_id(), Some(room.id()));
            assert_eq!(original_room.audience(), room.audience());
            assert_eq!(original_room.time().map(|t| *t.start()), Ok(started_at));
            assert_eq!(
                original_room.time().map(|t| t.end().to_owned()),
                Ok(RoomTimeBound::Excluded(started_at + duration))
            );
            assert_eq!(original_room.tags(), room.tags());

            // Assert original room events.
            let mut conn = db.get_conn().await;
            assert_events_original_room(&mut conn, &original_room).await;

            // Assert modified room.
            assert_eq!(modified_room.source_room_id(), Some(original_room.id()));
            assert_eq!(modified_room.audience(), original_room.audience());
            assert_eq!(modified_room.time(), original_room.time());
            assert_eq!(modified_room.tags(), original_room.tags());

            assert_events_modified_room(&mut conn, &modified_room).await;

            // Assert modified segments.
            let segments: Vec<(Bound<i64>, Bound<i64>)> = modified_segments.into();

            assert_eq!(
                segments,
                vec![
                    (Bound::Included(0), Bound::Excluded(6000)),
                    (Bound::Included(8000), Bound::Excluded(9500)),
                ]
            )
        });
    }

    async fn assert_events_original_room(mut conn: &mut PgConnection, original_room: &Room) {
        let events = EventListQuery::new()
            .room_id(original_room.id())
            .execute(&mut conn)
            .await
            .expect("Failed to fetch original room events");

        assert_eq!(events.len(), 12);

        assert_event(
            &events[0],
            // Bump to the first segment beginning because it's before the first segment.
            // 3e9 (offset)
            3_000_000_000,
            "message",
            &json!({"message": "m1"}),
        );

        assert_event(
            &events[1],
            // 12e9 - 10e9 (rtc offset) + 3e9 (offset)
            5_000_000_000,
            "message",
            &json!({"message": "m2"}),
        );

        assert_event(
            &events[2],
            // 13e9 - 10e9 (rtc offset) + 3e9 (offset)
            6_000_000_000,
            "stream",
            &json!({"cut": "start"}),
        );

        assert_event(
            &events[3],
            // 14e9 - 10e9 (rtc offset) + 3e9 (offset)
            7_000_000_000,
            "message",
            &json!({"message": "m4"}),
        );

        assert_event(
            &events[4],
            // 15e9 - 10e9 (rtc offset) + 3e9 (offset)
            8_000_000_000,
            "stream",
            &json!({"cut": "stop"}),
        );

        assert_event(
            &events[5],
            // 16e9 - 10e9 (rtc offset) + 3e9 (offset)
            9_000_000_000,
            "message",
            &json!({"message": "m6"}),
        );

        assert_event(
            &events[6],
            // 17e9 - (17e9 - 16.5e9) (gap part) - 10e9 (rtc offset) + 3e9 (offset)
            9_500_000_000,
            "message",
            &json!({"message": "m7"}),
        );

        assert_event(
            &events[7],
            // 18e9 - (18e9 - 16.5e9) (gap part) - 10e9 (rtc offset) + 3e9 (offset) + 1 (monotonize)
            9_500_000_001,
            "stream",
            &json!({"cut": "start"}),
        );

        assert_event(
            &events[8],
            // 19e9 - 2e9 (gap) - 10e9 (rtc offset) + 3e9 (offset)
            10_000_000_000,
            "message",
            &json!({"message": "m9"}),
        );

        assert_event(
            &events[9],
            // 20e9 - 2e9 (gap) - 10e9 (rtc offset) + 3e9 (offset)
            11_000_000_000,
            "stream",
            &json!({"cut": "stop"}),
        );

        assert_event(
            &events[10],
            // 21e9 - 2e9 (gap) - 10e9 (rtc offset) + 3e9 (offset)
            12_000_000_000,
            "message",
            &json!({"message": "m11"}),
        );

        assert_event(
            &events[11],
            // 22e9 - 2e9 (gap) - (22e9 - 21.5e9) (gap part) - 10e9 (rtc offset) + 3e9 (offset)
            12_500_000_000,
            "message",
            &json!({"message": "m12"}),
        );
    }

    async fn assert_events_modified_room(mut conn: &mut PgConnection, modified_room: &Room) {
        // Assert modified room events.
        let events = EventListQuery::new()
            .room_id(modified_room.id())
            .execute(&mut conn)
            .await
            .expect("Failed to fetch modified room events");

        assert_eq!(events.len(), 8);

        assert_event(
            &events[0],
            // No change
            3_000_000_000,
            "message",
            &json!({"message": "m1"}),
        );

        assert_event(
            &events[1],
            // No change
            5_000_000_000,
            "message",
            &json!({"message": "m2"}),
        );

        assert_event(
            &events[2],
            // 7e9 - (17e9 - 16e9) (cut 1 part) + 1 (monotonize)
            6_000_000_001,
            "message",
            &json!({"message": "m4"}),
        );

        assert_event(
            &events[3],
            // 9e9 - 2e9 (cut 1)
            7_000_000_000,
            "message",
            &json!({"message": "m6"}),
        );

        assert_event(
            &events[4],
            // 9.5e9 - 2e9 (cut 1)
            7_500_000_000,
            "message",
            &json!({"message": "m7"}),
        );

        assert_event(
            &events[5],
            // 10e9 - 2e9 (cut 1) - (20e9 - 19.5e9) (cut 2 part) + 2 (monotonize)
            7_500_000_002,
            "message",
            &json!({"message": "m9"}),
        );

        assert_event(
            &events[6],
            // 12e9 - 2e9 (cut 1) - 1.5e9 (cut 2) + 1 (monotonize)
            8_500_000_001,
            "message",
            &json!({"message": "m11"}),
        );

        assert_event(
            &events[7],
            // 12.5e9 - 2e9 (cut 1) - 1.5e9 (cut 2) + 1 (monotonize)
            9_000_000_001,
            "message",
            &json!({"message": "m12"}),
        );
    }

    async fn create_events(mut conn: &mut PgConnection, room: &Room) {
        // Create events.
        create_event(
            &mut conn,
            &room,
            1_000_000_000,
            "message",
            json!({"message": "m1"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            12_000_000_000,
            "message",
            json!({"message": "m2"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            13_000_000_000,
            "stream",
            json!({"cut": "start"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            14_000_000_000,
            "message",
            json!({"message": "m4"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            15_000_000_000,
            "stream",
            json!({"cut": "stop"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            16_000_000_000,
            "message",
            json!({"message": "m6"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            17_000_000_000,
            "message",
            json!({"message": "m7"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            18_000_000_000,
            "stream",
            json!({"cut": "start"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            19_000_000_000,
            "message",
            json!({"message": "m9"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            20_000_000_000,
            "stream",
            json!({"cut": "stop"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            21_000_000_000,
            "message",
            json!({"message": "m11"}),
        )
        .await;

        create_event(
            &mut conn,
            &room,
            22_000_000_000,
            "message",
            json!({"message": "m12"}),
        )
        .await;
    }

    async fn create_event(
        conn: &mut PgConnection,
        room: &Room,
        occurred_at: i64,
        kind: &str,
        data: JsonValue,
    ) {
        let created_by = AgentId::new("test", AccountId::new("test", AUDIENCE));

        let opened_at = match room.time().map(|t| t.into()) {
            Ok((Bound::Included(opened_at), _)) => opened_at,
            _ => panic!("Invalid room time"),
        };

        EventInsertQuery::new(
            room.id(),
            kind.to_owned(),
            data.clone(),
            occurred_at,
            created_by,
        )
        .created_at(opened_at + Duration::nanoseconds(occurred_at))
        .execute(conn)
        .await
        .expect("Failed to insert event");
    }

    fn assert_event(event: &Event, occurred_at: i64, kind: &str, data: &JsonValue) {
        assert_eq!(event.kind(), kind);
        assert_eq!(event.data(), data);
        assert_eq!(event.occurred_at(), occurred_at);
    }
}
