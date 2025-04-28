use diesel::{r2d2, sql_query, PgConnection, RunQueryDsl};

use crate::utils::num_cpus;

#[derive(Debug, Clone)]
pub struct Pool {
    inner: r2d2::Pool<r2d2::ConnectionManager<PgConnection>>,
}

impl Default for Pool {
    fn default() -> Self {
        let datasource = option_env!("DATABASE_URL").expect("no DATABASE_URL env");
        let mut active_num = num_cpus();
        if active_num == 0 {
            active_num = 1;
        }

        Self::new(datasource, active_num * 2)
    }
}

macro_rules! pool_query_dsl {
    ($name:ident,$ret_val:ty) => {
        #[cfg(feature = "diesel-enable")]
        impl Pool {
            pub fn $name<'query, U, DSL>(&self, dsl: DSL) -> diesel::QueryResult<$ret_val>
            where
                DSL: diesel::query_dsl::LoadQuery<
                        'query,
                        r2d2::PooledConnection<r2d2::ConnectionManager<PgConnection>>,
                        U,
                    > + diesel::RunQueryDsl<
                        r2d2::PooledConnection<r2d2::ConnectionManager<PgConnection>>,
                    >,
            {
                self.get_active_conn()
                    .map(|mut connection| dsl.$name(&mut connection).map(|r| r))
                    .unwrap_or(Err(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::UnableToSendCommand,
                        Box::new("no active connection".to_string()),
                    )))
            }
        }
    };
}

#[cfg(feature = "diesel-enable")]
impl Pool {
    pub fn new(datasource: &str, max_num: usize) -> Self {
        let conn_manager = r2d2::ConnectionManager::<PgConnection>::new(datasource);
        let pool = r2d2::Pool::builder()
            .max_size(max_num as u32)
            .build(conn_manager)
            .expect("create pool failed");
        Self { inner: pool }
    }

    pub fn run_pending_migrations(
        &self,
        source: diesel_migrations::EmbeddedMigrations,
    ) -> diesel::migration::Result<Vec<String>> {
        use diesel_migrations::MigrationHarness;

        let mut conn = self.get_active_conn().expect("no active connection");
        let result = conn.run_pending_migrations(source);

        result.map(|migrate_versions| {
            migrate_versions
                .iter()
                .map(|version| version.to_string())
                .collect()
        })
    }

    pub fn execute<DSL>(&self, dsl: DSL) -> diesel::QueryResult<usize>
    where
        DSL: diesel::query_dsl::methods::ExecuteDsl<
                r2d2::PooledConnection<r2d2::ConnectionManager<PgConnection>>,
            > + diesel::RunQueryDsl<r2d2::PooledConnection<r2d2::ConnectionManager<PgConnection>>>,
    {
        self.get_active_conn()
            .map(|mut connection| dsl.execute(&mut connection).map(|r| r))
            .unwrap_or(Err(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new("no active connection".to_string()),
            )))
    }

    pub fn load_by_sql<'query, U>(&self, sql: &str) -> diesel::QueryResult<Vec<U>>
    where
        U: diesel::QueryableByName<diesel::pg::Pg> + 'static,
    {
        self.get_active_conn()
            .map(|mut connection| sql_query(sql).load(&mut connection).map(|r| r))
            .unwrap_or(Err(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new("no active connection".to_string()),
            )))
    }
}

pool_query_dsl!(load, Vec<U>);
pool_query_dsl!(get_result, U);
pool_query_dsl!(get_results, Vec<U>);

impl Pool {
    fn get_active_conn(
        &self,
    ) -> Option<r2d2::PooledConnection<r2d2::ConnectionManager<PgConnection>>> {
        if self.inner.state().idle_connections == 0 {
            None
        } else {
            self.inner
                .get()
                .map(|conn| conn)
                .map_err(|err| tracing::error!("get connection failed: {}", err))
                .ok()
        }
    }
}
