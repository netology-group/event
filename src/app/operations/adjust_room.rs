use std::cmp;
use std::ops::Bound;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use diesel::pg::PgConnection;
use log::info;

use crate::db::adjustment::{InsertQuery as AdjustmentInsertQuery, Segment};
use crate::db::event::{
    DeleteQuery as EventDeleteQuery, ListQuery as EventListQuery, Object as Event,
};
use crate::db::room::{InsertQuery as RoomInsertQuery, Object as Room};
use crate::db::ConnectionPool as Db;

pub(crate) const NANOSECONDS_IN_MILLISECOND: i64 = 1_000_000;

pub(crate) fn call(
    db: &Db,
    real_time_room: &Room,
    started_at: DateTime<Utc>,
    segments: &[Segment],
    offset: i64,
) -> Result<(Room, Room, Vec<Segment>)> {
    info!(
        "Room adjustment task started for room_id = '{}'",
        real_time_room.id()
    );

    let start_timestamp = Utc::now();
    let conn = db.get()?;

    // Create adjustment.
    AdjustmentInsertQuery::new(real_time_room.id(), started_at, segments.to_vec(), offset)
        .execute(&conn)?;

    ///////////////////////////////////////////////////////////////////////////

    // Get room opening time and duration.
    let (room_opening, room_duration) = match real_time_room.time() {
        (Bound::Included(start), Bound::Excluded(stop)) if stop > start => {
            (*start, stop.signed_duration_since(*start))
        }
        _ => bail!("invalid duration for room = '{}'", real_time_room.id()),
    };

    // Calculate RTC offset as the difference between event room opening and RTC start.
    let rtc_offset = (started_at - room_opening).num_milliseconds();

    // Convert segments to nanoseconds.
    let mut nano_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        if let (Bound::Included(start), Bound::Excluded(stop)) = segment {
            let nano_start = (start + rtc_offset) * NANOSECONDS_IN_MILLISECOND;
            let nano_stop = (stop + rtc_offset) * NANOSECONDS_IN_MILLISECOND;
            nano_segments.push((Bound::Included(nano_start), Bound::Excluded(nano_stop)));
        } else {
            bail!("invalid segment");
        }
    }

    // Invert segments to gaps.
    let segment_gaps = invert_segments(&nano_segments, room_duration)?;

    // Create original room with events shifted according to segments.
    let original_room = create_room(&conn, &real_time_room, started_at)?;

    clone_events(
        &conn,
        &original_room,
        &segment_gaps,
        (offset - rtc_offset) * NANOSECONDS_IN_MILLISECOND,
    )?;

    ///////////////////////////////////////////////////////////////////////////

    // Fetch shifted cut events and transform them to gaps.
    let cut_events = EventListQuery::new()
        .room_id(original_room.id())
        .kind("stream")
        .execute(&conn)
        .with_context(|| {
            format!(
                "failed to fetch cut events for room_id = '{}'",
                original_room.id()
            )
        })?;

    let cut_gaps = cut_events_to_gaps(&cut_events)?;

    // Create modified room with events shifted again according to cut events this time.
    let modified_room = create_room(&conn, &original_room, started_at)?;
    clone_events(&conn, &modified_room, &cut_gaps, 0)?;

    // Delete cut events from the modified room.
    EventDeleteQuery::new(modified_room.id(), "stream")
        .execute(&conn)
        .with_context(|| {
            format!(
                "failed to delete cut events for room_id = '{}'",
                modified_room.id()
            )
        })?;

    ///////////////////////////////////////////////////////////////////////////

    // Calculate total duration of initial segments.
    let mut total_segments_millis = 0;

    for segment in segments {
        if let (Bound::Included(start), Bound::Excluded(stop)) = segment {
            total_segments_millis += stop - start
        }
    }

    let total_segments_duration = Duration::milliseconds(total_segments_millis);

    // Calculate modified segments by inverting cut gaps limited by total initial segments duration.
    let bounded_cut_gaps = cut_gaps
        .into_iter()
        .map(|(start, stop)| (Bound::Included(start), Bound::Excluded(stop)))
        .collect::<Vec<Segment>>();

    let modified_segments = invert_segments(&bounded_cut_gaps, total_segments_duration)?
        .into_iter()
        .map(|(start, stop)| {
            (
                Bound::Included(cmp::max(start / NANOSECONDS_IN_MILLISECOND, 0)),
                Bound::Excluded(stop / NANOSECONDS_IN_MILLISECOND),
            )
        })
        .collect::<Vec<Segment>>();

    ///////////////////////////////////////////////////////////////////////////

    // Done.
    info!(
        "Room adjustment task successfully finished for room_id = '{}', duration = {} ms",
        real_time_room.id(),
        (Utc::now() - start_timestamp).num_milliseconds()
    );

    Ok((original_room, modified_room, modified_segments))
}

/// Creates a derived room from the source room.
fn create_room(conn: &PgConnection, source_room: &Room, started_at: DateTime<Utc>) -> Result<Room> {
    let time = (Bound::Included(started_at), source_room.time().1.to_owned());
    let mut query = RoomInsertQuery::new(&source_room.audience(), time);
    query = query.source_room_id(source_room.id());

    if let Some(tags) = source_room.tags() {
        query = query.tags(tags.to_owned());
    }

    query.execute(conn).context("failed to insert room")
}

const SHIFT_CLONE_EVENTS_SQL: &str = r#"
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
        $3 AS room_id,
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
"#;

/// Clones events from the source room of the `room` with shifting them according to `gaps` and
/// adding `offset` (both in nanoseconds).
fn clone_events(conn: &PgConnection, room: &Room, gaps: &[(i64, i64)], offset: i64) -> Result<()> {
    use diesel::prelude::*;
    use diesel::sql_types::{Array, Int8, Uuid};

    let source_room_id = match room.source_room_id() {
        Some(id) => id,
        None => bail!("room = '{}' is not derived from another room", room.id()),
    };

    let mut starts = Vec::with_capacity(gaps.len());
    let mut stops = Vec::with_capacity(gaps.len());

    for (start, stop) in gaps {
        starts.push(start);
        stops.push(stop);
    }

    diesel::sql_query(SHIFT_CLONE_EVENTS_SQL)
        .bind::<Array<Int8>, _>(&starts)
        .bind::<Array<Int8>, _>(&stops)
        .bind::<Uuid, _>(room.id())
        .bind::<Int8, _>(offset)
        .bind::<Uuid, _>(source_room_id)
        .execute(conn)
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
    segments: &[Segment],
    room_duration: Duration,
) -> Result<Vec<(i64, i64)>> {
    if segments.is_empty() {
        let total_nanos = room_duration.num_nanoseconds().unwrap_or(std::i64::MAX);
        return Ok(vec![(0, total_nanos)]);
    }

    let mut gaps = Vec::with_capacity(segments.len() + 2);

    // A possible gap before the first segment.
    if let Some(first_segment) = segments.first() {
        match first_segment {
            (Bound::Included(first_segment_start), _) => {
                if *first_segment_start > 0 {
                    gaps.push((0, *first_segment_start));
                }
            }
            _ => bail!("invalid first segment"),
        }
    }

    // Gaps between segments.
    for item in segments.iter().zip(&segments[1..]) {
        match item {
            ((_, Bound::Excluded(segment_stop)), (Bound::Included(next_segment_start), _)) => {
                gaps.push((*segment_stop, *next_segment_start));
            }
            _ => bail!("invalid segments"),
        }
    }

    // A possible gap after the last segment.
    if let Some(last_segment) = segments.last() {
        match last_segment {
            (_, Bound::Excluded(last_segment_stop)) => {
                let room_duration_nanos = room_duration.num_nanoseconds().unwrap_or(std::i64::MAX);

                if *last_segment_stop < room_duration_nanos {
                    gaps.push((*last_segment_stop, room_duration_nanos));
                }
            }
            _ => bail!("invalid last segment"),
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

    use crate::db::event::{ListQuery as EventListQuery, Object as Event};
    use crate::db::room::InsertQuery as RoomInsertQuery;
    use crate::test_helpers::db::TestDb;
    use crate::test_helpers::prelude::*;

    const AUDIENCE: &str = "dev.svc.example.org";

    #[test]
    fn adjust_room() {
        futures::executor::block_on(async {
            let db = TestDb::new();

            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get db connection");

            // Create room.
            let opened_at = DateTime::from_utc(NaiveDateTime::from_timestamp(1582002673, 0), Utc);
            let closed_at = opened_at + Duration::seconds(50);
            let time = (Bound::Included(opened_at), Bound::Excluded(closed_at));

            let room = RoomInsertQuery::new(AUDIENCE, time)
                .execute(&conn)
                .expect("Failed to insert room");

            // Create events.
            shared_helpers::create_event(
                &conn,
                &room,
                1_000_000_000,
                "message",
                json!({"message": "m1"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                12_000_000_000,
                "message",
                json!({"message": "m2"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                13_000_000_000,
                "stream",
                json!({"cut": "start"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                14_000_000_000,
                "message",
                json!({"message": "m4"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                15_000_000_000,
                "stream",
                json!({"cut": "stop"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                16_000_000_000,
                "message",
                json!({"message": "m6"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                17_000_000_000,
                "message",
                json!({"message": "m7"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                18_000_000_000,
                "stream",
                json!({"cut": "start"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                19_000_000_000,
                "message",
                json!({"message": "m9"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                20_000_000_000,
                "stream",
                json!({"cut": "stop"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                21_000_000_000,
                "message",
                json!({"message": "m11"}),
            );

            shared_helpers::create_event(
                &conn,
                &room,
                22_000_000_000,
                "message",
                json!({"message": "m12"}),
            );

            drop(conn);

            // Video segments.
            let segments = vec![
                (Bound::Included(0), Bound::Excluded(6500)),
                (Bound::Included(8500), Bound::Excluded(11500)),
            ];

            // RTC started 10 seconds after the room opened and preroll is 3 seconds long.
            let started_at = opened_at + Duration::seconds(10);
            let offset = 3000;

            // Call room adjustment.
            let (original_room, modified_room, modified_segments) =
                super::call(db.connection_pool(), &room, started_at, &segments, offset)
                    .expect("Room adjustment failed");

            // Assert original room.
            assert_eq!(original_room.source_room_id(), Some(room.id()));
            assert_eq!(original_room.audience(), room.audience());
            assert_eq!(original_room.time().0, Bound::Included(started_at));
            assert_eq!(original_room.time().1, room.time().1);
            assert_eq!(original_room.tags(), room.tags());

            // Assert original room events.
            let conn = db
                .connection_pool()
                .get()
                .expect("Failed to get db connection");

            let events = EventListQuery::new()
                .room_id(original_room.id())
                .execute(&conn)
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

            // Assert modified room.
            assert_eq!(modified_room.source_room_id(), Some(original_room.id()));
            assert_eq!(modified_room.audience(), original_room.audience());
            assert_eq!(modified_room.time(), original_room.time());
            assert_eq!(modified_room.tags(), original_room.tags());

            // Assert modified room events.
            let events = EventListQuery::new()
                .room_id(modified_room.id())
                .execute(&conn)
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

            // Assert modified segments.
            assert_eq!(
                modified_segments,
                vec![
                    (Bound::Included(0), Bound::Excluded(6000)),
                    (Bound::Included(8000), Bound::Excluded(9500)),
                ]
            )
        });
    }

    fn assert_event(event: &Event, occurred_at: i64, kind: &str, data: &JsonValue) {
        assert_eq!(event.kind(), kind);
        assert_eq!(event.data(), data);
        assert_eq!(event.occurred_at(), occurred_at);
    }
}
