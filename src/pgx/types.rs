use std::{fmt, result};
use std::error::Error;

use bytes::BytesMut;
use serde::de::StdError;

pub type Result<T> = result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy)]
pub struct UnexpectedNullError;

pub struct Json(serde_json::Value);
pub struct Jsonb(serde_json::Value);
pub struct Uuid(uuid::Uuid);

impl fmt::Display for UnexpectedNullError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unexpected null for non-null column")
    }
}

impl StdError for UnexpectedNullError {}

#[cold]
#[inline(never)]
fn emit_size_error<T>(var_name: &str) -> Result<T> {
    Err(var_name.into())
}

pub trait FromSql: Sized {
    fn from_sql(value: &[u8]) -> Result<Self>;

    #[inline(always)]
    fn from_nullable_sql(bytes: Option<&[u8]>) -> Result<Self> {
        match bytes {
            Some(bytes) => Self::from_sql(bytes),
            None => Err(Box::new(UnexpectedNullError)),
        }
    }
}

pub trait ToSql {
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()>;
}

pub trait SqlType {
    fn ty_oid() -> u32;
}

impl<'a, T> ToSql for &'a T
where
    T: ToSql,
{
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()> {
        (*self).to_sql(buf)
    }
}

impl<'a, T> ToSql for &'a mut T
where
    T: ToSql,
{
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()> {
        (**self).to_sql(buf)
    }
}

impl<'a, T> ToSql for Box<T>
where
    T: ToSql,
{
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()> {
        self.as_ref().to_sql(buf)
    }
}

impl<T> ToSql for Option<T>
where
    T: ToSql,
{
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Some(v) => v.to_sql(buf),
            None => Err(Box::new(UnexpectedNullError)),
        }
    }
}

impl<T> ToSql for Vec<T>
where
    T: ToSql,
{
    fn to_sql(&self, buf: &mut BytesMut) -> Result<()> {
        for v in self {
            v.to_sql(buf)?;
        }
        Ok(())
    }
}

macro_rules! impl_sql_type {
    ($ty:ty,$oid:expr) => {
        impl SqlType for $ty {
            fn ty_oid() -> u32 {
                $oid
            }
        }
    };
}

impl_sql_type!(String, 1043);
impl_sql_type!(Json, 114);
impl_sql_type!(Jsonb, 3802);
impl_sql_type!(Uuid, 2950);

impl FromSql for String {
    fn from_sql(value: &[u8]) -> Result<Self> {
        String::from_utf8_lossy(value)
            .parse()
            .map_err(|e| Box::new(e) as Box<_>)
    }
}

impl<'a> SqlType for &'a str {
    fn ty_oid() -> u32 {
        1043
    }
}

pub mod primitive {
    use byteorder::{NetworkEndian, ReadBytesExt};

    use super::*;

    impl_sql_type!(bool, 16);
    impl_sql_type!(i64, 21);
    impl_sql_type!(i16, 21);
    impl_sql_type!(i32, 23);
    impl_sql_type!(f32, 700);
    impl_sql_type!(f64, 701);

    impl FromSql for bool {
        fn from_sql(value: &[u8]) -> Result<Self> {
            Ok(value[0] != 0)
        }
    }

    impl FromSql for i16 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;
            if bytes.len() < 2 {
                return emit_size_error(
                    "Received less than 2 bytes while decoding an i16. \
                    Was an expression of a different type accidentally marked as SmallInt?",
                );
            }

            if bytes.len() > 2 {
                return emit_size_error(
                    "Received more than 2 bytes while decoding an i16. \
                    Was an Integer expression accidentally marked as SmallInt?",
                );
            }
            bytes
                .read_i16::<NetworkEndian>()
                .map_err(|e| Box::new(e) as Box<_>)
        }
    }

    impl FromSql for u32 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;
            bytes.read_u32::<NetworkEndian>().map_err(Into::into)
        }
    }

    impl FromSql for i32 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;
            if bytes.len() < 4 {
                return emit_size_error(
                    "Received less than 4 bytes while decoding an i32. \
                    Was an SmallInt expression accidentally marked as Integer?",
                );
            }

            if bytes.len() > 4 {
                return emit_size_error(
                    "Received more than 4 bytes while decoding an i32. \
                    Was an BigInt expression accidentally marked as Integer?",
                );
            }
            bytes
                .read_i32::<NetworkEndian>()
                .map_err(|e| Box::new(e) as Box<_>)
        }
    }

    impl FromSql for i64 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;
            if bytes.len() < 8 {
                return emit_size_error(
                    "Received less than 8 bytes while decoding an i64. \
                    Was an Integer expression accidentally marked as BigInt?",
                );
            }

            if bytes.len() > 8 {
                return emit_size_error(
                    "Received more than 8 bytes while decoding an i64. \
                    Was an expression of a different type expression accidentally marked as BigInt?"
                );
            }
            bytes
                .read_i64::<NetworkEndian>()
                .map_err(|e| Box::new(e) as Box<_>)
        }
    }

    impl FromSql for f32 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;

            if bytes.len() < 4 {
                return Err("Received less than 4 bytes while decoding an f32. \
                 Was a numeric accidentally marked as float?"
                    .into());
            }

            if bytes.len() > 4 {
                return Err("Received more than 4 bytes while decoding an f32. \
                 Was a double accidentally marked as float?"
                    .into());
            }

            bytes
                .read_f32::<NetworkEndian>()
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        }
    }

    impl FromSql for f64 {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;

            if bytes.len() < 8 {
                return Err("Received less than 8 bytes while decoding an f64. \
                    Was a float accidentally marked as double?"
                    .into());
            }

            if bytes.len() > 8 {
                return Err("Received more than 8 bytes while decoding an f64. \
                    Was a numeric accidentally marked as double?"
                    .into());
            }

            bytes
                .read_f64::<NetworkEndian>()
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
        }
    }
}

pub mod datetime {
    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

    use crate::pgx::types::FromSql;

    use super::*;

    impl_sql_type!(NaiveTime, 1083);
    impl_sql_type!(NaiveDate, 1082);
    impl_sql_type!(NaiveDateTime, 1114);

    // Postgres timestamps start from January 1st 2000.
    fn pg_epoch() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("This is in supported range of chrono dates")
            .and_hms_opt(0, 0, 0)
            .expect("This is a valid input")
    }

    fn pg_epoch_date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2000, 1, 1).expect("This is in supported range of chrono dates")
    }

    pub fn midnight() -> NaiveTime {
        NaiveTime::from_hms_opt(0, 0, 0).expect("This is a valid hms spec")
    }

    impl FromSql for NaiveDateTime {
        fn from_sql(value: &[u8]) -> Result<Self> {
            i64::from_sql(value).and_then(|offset| {
                match pg_epoch().checked_add_signed(Duration::microseconds(offset)) {
                    Some(v) => Ok(v),
                    None => {
                        let message =
                            "Tried to deserialize a timestamp that is too large for Chrono";
                        Err(message.into())
                    }
                }
            })
        }
    }

    impl FromSql for NaiveDate {
        fn from_sql(value: &[u8]) -> Result<Self> {
            i32::from_sql(value).and_then(|offset| {
                #[allow(deprecated)] // otherwise we would need to bump our minimal chrono version
                let duration = Duration::days(i64::from(offset));
                match pg_epoch_date().checked_add_signed(duration) {
                    Some(date) => Ok(date),
                    None => {
                        let error_message =
                            format!("Chrono can only represent dates up to {:?}", NaiveDate::MAX);
                        Err(error_message.into())
                    }
                }
            })
        }
    }

    impl FromSql for NaiveTime {
        fn from_sql(value: &[u8]) -> Result<Self> {
            i64::from_sql(value).and_then(|offset| {
                let duration = Duration::microseconds(offset);
                Ok(midnight() + duration)
            })
        }
    }
}

pub mod complex {
    use byteorder::{NetworkEndian, ReadBytesExt};
    use bytes::Bytes;

    use super::*;

    impl FromSql for Vec<u8> {
        fn from_sql(value: &[u8]) -> Result<Self> {
            Ok(value.to_vec())
        }
    }

    impl<V> FromSql for Vec<V>
    where
        V: FromSql,
    {
        fn from_sql(value: &[u8]) -> Result<Self> {
            let mut bytes = value;
            let num_dimensions = bytes.read_i32::<NetworkEndian>()?;
            let has_null = bytes.read_i32::<NetworkEndian>()? != 0;
            let _oid = bytes.read_i32::<NetworkEndian>()?;

            if num_dimensions == 0 {
                return Ok(Vec::new());
            }

            let num_elements = bytes.read_i32::<NetworkEndian>()?;
            let _lower_bound = bytes.read_i32::<NetworkEndian>()?;

            if num_dimensions != 1 {
                return Err("multi-dimensional arrays are not supported".into());
            }

            (0..num_elements)
                .map(|_| {
                    let elem_size = bytes.read_i32::<NetworkEndian>()?;
                    if has_null && elem_size == -1 {
                        V::from_nullable_sql(None)
                    } else {
                        let (elem_bytes, new_bytes) = bytes.split_at(elem_size.try_into()?);
                        bytes = new_bytes;
                        V::from_sql(elem_bytes)
                    }
                })
                .collect()
        }
    }

    impl<'a> SqlType for &'a [u8] {
        fn ty_oid() -> u32 {
            17
        }
    }

    impl_sql_type!(Vec<i16>, 22);
    impl_sql_type!(Vec<u8>, 17);
    impl_sql_type!(BytesMut, 17);
    impl_sql_type!(Bytes, 17);
}
