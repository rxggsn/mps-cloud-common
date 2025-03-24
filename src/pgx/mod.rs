#[cfg(feature = "diesel-enable")]
pub mod diesel;
pub mod tokio;

#[cfg(feature = "sqlx-enable")]
use sqlx::postgres::{PgColumn, PgRow};
#[cfg(feature = "sqlx-enable")]
pub fn stringify_column(col: &PgColumn, row: &PgRow) -> Result<String, sqlx::Error> {
    use postgres_types::Type;
    use sqlx::{Column, Row};
    if let Some(sqlx::postgres::types::Oid(id)) = col.type_info().oid() {
        if id == Type::BOOL.oid() {
            Ok(row.get::<bool, _>(col.ordinal()).to_string())
        } else if Type::INT2.oid() == id {
            Ok(row.get::<i16, _>(col.ordinal()).to_string())
        } else if Type::INT4.oid() == id {
            Ok(row.get::<i32, _>(col.ordinal()).to_string())
        } else if Type::INT8.oid() == id {
            row.try_get::<i64, _>(col.ordinal())
                .map(|val| val.to_string())
        } else if Type::FLOAT4.oid() == id {
            row.try_get::<f32, _>(col.ordinal())
                .map(|val| val.to_string())
        } else if Type::FLOAT8.oid() == id {
            row.try_get::<f64, _>(col.ordinal())
                .map(|val| val.to_string())
        } else if Type::NUMERIC.oid() == id {
            row.try_get::<bigdecimal::BigDecimal, _>(col.ordinal())
                .map(|val| val.to_string())
        } else if Type::CHAR.oid() == id
            || Type::VARCHAR.oid() == id
            || Type::NAME.oid() == id
            || Type::TEXT.oid() == id
        {
            row.try_get::<String, _>(col.ordinal())
        } else if Type::JSON.oid() == id || Type::JSONB.oid() == id {
            serde_json::to_string(&row.try_get::<serde_json::Value, _>(col.ordinal())?)
                .map_err(|err| sqlx::Error::Decode(err.to_string().into()))
        } else if Type::DATE.oid() == id {
            row.try_get::<chrono::NaiveDate, _>(col.ordinal())
                .map(|val| val.format(crate::times::CHRONO_DATE_FORMAT).to_string())
        } else if Type::TIME.oid() == id || Type::TIMETZ.oid() == id {
            row.try_get::<chrono::NaiveTime, _>(col.ordinal())
                .map(|val| val.format(crate::times::CHRONO_TIME_FORMAT).to_string())
        } else if Type::TIMESTAMP.oid() == id || Type::TIMESTAMPTZ.oid() == id {
            row.try_get::<chrono::NaiveDateTime, _>(col.ordinal())
                .map(|val| {
                    val.format(crate::times::CHRONO_DATE_TIME_FORMAT)
                        .to_string()
                })
        } else {
            Ok("".to_string())
        }
    } else {
        Ok("".to_string())
    }
}
