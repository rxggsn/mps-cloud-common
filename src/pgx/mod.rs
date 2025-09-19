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
        } else if Type::INTERVAL.oid() == id {
            row.try_get::<sqlx::postgres::types::PgInterval, _>(col.ordinal())
                .map(|val| {
                    format!(
                        "{} months, {} days, {} hours, {} minutes and {} seconds",
                        val.months,
                        val.days,
                        val.microseconds / 1_000_000 / 60 / 60,
                        (val.microseconds / 1_000_000 / 60) % 60,
                        val.microseconds / 1_000_000 % 60
                    )
                })
        } else if Type::BYTEA.oid() == id {
            let raw_data = row.try_get_raw(col.ordinal())?;
            let bytes = raw_data.as_bytes().map_err(sqlx::Error::Decode)?;
            Ok(base64::encode(bytes))
        } else if Type::UUID.oid() == id {
            row.try_get::<uuid::Uuid, _>(col.ordinal())
                .map(|val| val.to_string())
        } else {
            match col.type_info().kind() {
                sqlx::postgres::PgTypeKind::Enum(enums) => {
                    let raw_data = row.try_get_raw(col.ordinal())?;
                    String::from_utf8(raw_data.as_bytes().map_err(sqlx::Error::Decode)?.to_vec())
                        .map_err(|e| sqlx::Error::Decode(e.to_string().into()))
                }
                _ => {
                    // Fallback to string representation for other types
                    Err(sqlx::Error::Decode(
                        format!("Unsupported column type with OID {}", id).into(),
                    ))
                }
            }
        }
    } else {
        row.try_get::<String, _>(col.ordinal())
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    #[cfg(feature = "sqlx-enable")]
    async fn test_stringify_column() {
        use sqlx::{Column, Row};
        tracing_subscriber::fmt::init();
        const QUERY: &str = r#"SELECT coupon_type AS "优惠券类型", COUNT(*) AS "发放数量", SUM(redeem_amount) AS "累计抵扣金额", AVG(redeem_amount) AS "平均抵扣金额" FROM cac_redeem_extension WHERE redeem_type = 'REDEEM_OK' GROUP BY coupon_type"#;
        let pool = sqlx::PgPool::connect(
            "postgresql://postgres:postgres@localhost/rxdomain_alpha?sslmode=disable",
        )
        .await
        .unwrap();
        let rows = sqlx::query(QUERY)
            .fetch_all(&pool)
            .await
            .expect("query failed");
        for row in rows {
            for col in row.columns() {
                tracing::info!("Column: {}, oid: {:?}", col.name(), col.type_info().oid());
                let val = super::stringify_column(col, &row).expect("stringify column failed");
                tracing::info!("Column: {}, Value: {}", col.name(), val);
            }
        }
    }
}
