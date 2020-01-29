pub(crate) mod ts_seconds_bound_tuple {
    use std::fmt;
    use std::ops::Bound;

    use chrono::{DateTime, NaiveDateTime, Utc};
    use serde::{de, ser};

    pub(crate) fn serialize<S>(
        value: &(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>),
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use ser::SerializeTuple;

        let (lt, rt) = value;
        let mut tup = serializer.serialize_tuple(2)?;

        match lt {
            Bound::Included(lt) => {
                let val = lt.timestamp();
                tup.serialize_element(&val)?;
            }
            Bound::Excluded(lt) => {
                // Adjusting the range to '[lt, rt)'
                let val = lt.timestamp() + 1;
                tup.serialize_element(&val)?;
            }
            Bound::Unbounded => {
                let val: Option<i64> = None;
                tup.serialize_element(&val)?;
            }
        }

        match rt {
            Bound::Included(rt) => {
                // Adjusting the range to '[lt, rt)'
                let val = rt.timestamp() - 1;
                tup.serialize_element(&val)?;
            }
            Bound::Excluded(rt) => {
                let val = rt.timestamp();
                tup.serialize_element(&val)?;
            }
            Bound::Unbounded => {
                let val: Option<i64> = None;
                tup.serialize_element(&val)?;
            }
        }

        tup.end()
    }

    pub fn deserialize<'de, D>(
        d: D,
    ) -> Result<(Bound<DateTime<Utc>>, Bound<DateTime<Utc>>), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        d.deserialize_tuple(2, TupleSecondsTimestampVisitor)
    }

    struct TupleSecondsTimestampVisitor;

    impl<'de> de::Visitor<'de> for TupleSecondsTimestampVisitor {
        type Value = (Bound<DateTime<Utc>>, Bound<DateTime<Utc>>);

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a [lt, rt) range of unix time (seconds) or null (unbounded)")
        }

        /// Deserialize a tuple of two Bounded DateTime<Utc>
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            let lt = match seq.next_element()? {
                Some(Some(val)) => {
                    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(val, 0), Utc);
                    Bound::Included(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(1, &self)),
            };

            let rt = match seq.next_element()? {
                Some(Some(val)) => {
                    let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(val, 0), Utc);
                    Bound::Excluded(dt)
                }
                Some(None) => Bound::Unbounded,
                None => return Err(de::Error::invalid_length(2, &self)),
            };

            return Ok((lt, rt));
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) mod milliseconds_bound_tuples {
    use std::fmt;
    use std::ops::Bound;

    use serde::{de, ser};

    pub(crate) fn serialize<S>(
        value: &Vec<(Bound<i64>, Bound<i64>)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        use ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(value.len()))?;

        for (lt, rt) in value {
            let lt = match lt {
                Bound::Included(lt) | Bound::Excluded(lt) => Some(lt),
                Bound::Unbounded => None,
            };

            let rt = match rt {
                Bound::Included(rt) | Bound::Excluded(rt) => Some(rt),
                Bound::Unbounded => None,
            };

            seq.serialize_element(&(lt, rt))?;
        }

        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Vec<(Bound<i64>, Bound<i64>)>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        pub struct MillisecondsBoundTupleVisitor;

        impl<'de> de::Visitor<'de> for MillisecondsBoundTupleVisitor {
            type Value = Vec<(Bound<i64>, Bound<i64>)>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a list of [lt, rt) range of integer milliseconds")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut elements: Self::Value = vec![];

                while let Some((Some(lt), Some(rt))) = seq.next_element()? {
                    if lt <= rt {
                        elements.push((Bound::Included(lt), Bound::Excluded(rt)))
                    } else {
                        return Err(de::Error::invalid_value(
                            de::Unexpected::Str(&format!("[{}, {}]", lt, rt)),
                            &"lt <= rt",
                        ));
                    }
                }

                Ok(elements)
            }
        }

        deserializer.deserialize_seq(MillisecondsBoundTupleVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::ops::Bound;

    use serde_derive::{Deserialize, Serialize};

    #[test]
    fn serialize_milliseconds_bound_tuples() {
        #[derive(Serialize)]
        struct Data {
            #[serde(with = "crate::serde::milliseconds_bound_tuples")]
            segments: Vec<(Bound<i64>, Bound<i64>)>,
        }

        let data = Data {
            segments: vec![
                (Bound::Included(0), Bound::Excluded(1000)),
                (Bound::Included(2000), Bound::Excluded(3000)),
            ],
        };

        let serialized = serde_json::to_string(&data).expect("Failed to serialize test data");
        let expected = r#"{"segments":[[0,1000],[2000,3000]]}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_milliseconds_bound_tuples() {
        #[derive(Deserialize)]
        struct Data {
            #[serde(with = "crate::serde::milliseconds_bound_tuples")]
            segments: Vec<(Bound<i64>, Bound<i64>)>,
        }

        let data = serde_json::from_str::<Data>(r#"{"segments": [[0, 1000], [2000, 3000]]}"#)
            .expect("Failed to deserialize test data");

        let expected = vec![
            (Bound::Included(0), Bound::Excluded(1000)),
            (Bound::Included(2000), Bound::Excluded(3000)),
        ];

        assert_eq!(data.segments, expected);
    }
}
