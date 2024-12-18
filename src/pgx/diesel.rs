use std::{sync::Arc, time::Duration};
use std::collections::BTreeMap;
use std::sync::Mutex;

use crossbeam_skiplist::SkipSet;
use derive_new::new;
use diesel::{Connection, PgConnection, QueryResult};
use diesel::connection::InstrumentationEvent;
use tokio::sync::oneshot;

use crate::concurrency::mutex;
use crate::utils::num_cpus;


#[derive(Clone)]
pub struct Pool {
    inner: Arc<BTreeMap<i32, Mutex<PgConnection>>>,
    unlocked_con: Arc<SkipSet<i32>>,
    max_num: usize,
    c_signal: Arc<oneshot::Sender<()>>,
}

impl Default for Pool {
    fn default() -> Self {
        let datasource = option_env!("DATABASE_URL").expect("no DATABASE_URL env");
        let mut active_num = num_cpus();
        if active_num == 0 {
            active_num = 1;
        }

        Self::new(datasource, active_num, active_num * 2)
    }
}

macro_rules! pool_query_dsl {
    ($name:ident,$ret_val:ty) => {
        #[cfg(feature = "diesel-enable")]
        impl Pool {
            pub fn $name<'query, U, DSL>(&self, dsl: DSL) -> diesel::QueryResult<$ret_val>
            where
                DSL: diesel::query_dsl::LoadQuery<'query, PgConnection, U>,
            {
                self.get_active_conn()
                    .map(|(id, connection)| {
                        let conn = &mut *mutex(connection);
                        dsl.$name(conn).map(|r| {
                            self.put_back_conn(id);
                            r
                        })
                    })
                    .unwrap_or(Err(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::UnableToSendCommand,
                        Box::new("no active connection".to_string()),
                    )))
            }
        }
    };
}

impl Pool {
    pub fn new(datasource: &str, active_num: usize, max_num: usize) -> Self {
        let mut inner = BTreeMap::default();
        let unlocked_con = SkipSet::from_iter(1i32..((active_num + 1) as i32));
        (0..active_num).for_each(|idx| {
            let connection = Self::new_connection(datasource);
            inner.insert((idx + 1) as i32, Mutex::new(connection));
        });

        let connections = Arc::new(inner);
        let c_connections = Arc::clone(&connections);
        let (tx, rx) = oneshot::channel();
        Self::keep_alive(c_connections, datasource, rx);
        Self {
            inner: connections,
            unlocked_con: Arc::new(unlocked_con),
            max_num,
            c_signal: Arc::new(tx),
        }
    }

    pub fn run_pending_migrations(
        &self,
        source: diesel_migrations::EmbeddedMigrations,
    ) -> diesel::migration::Result<Vec<String>> {
        use diesel_migrations::MigrationHarness;
        let (id, conn) = self.get_active_conn().expect("no active connection");

        let conn = &mut *mutex(conn);
        let result = conn.run_pending_migrations(source);

        self.put_back_conn(id);
        result.map(|migrate_versions| {
            migrate_versions
                .iter()
                .map(|version| version.to_string())
                .collect()
        })
    }

    pub fn execute<DSL>(&self, dsl: DSL) -> QueryResult<usize>
    where
        DSL: diesel::query_dsl::methods::ExecuteDsl<PgConnection>
            + diesel::RunQueryDsl<PgConnection>,
    {
        self.get_active_conn()
            .map(|(id, connection)| {
                let conn = &mut *mutex(connection);
                dsl.execute(conn).map(|r| {
                    self.put_back_conn(id);
                    r
                })
            })
            .unwrap_or(Err(diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::UnableToSendCommand,
                Box::new("no active connection".to_string()),
            )))
    }
}

pool_query_dsl!(load, Vec<U>);
pool_query_dsl!(get_result, U);

impl Pool {
    fn new_connection(datasource: &str) -> PgConnection {
        let mut connection = PgConnection::establish(datasource).expect("connect pg failed");
        connection.set_instrumentation(handle_instrumentation_event);
        connection
    }
    fn get_active_conn(&self) -> Option<(i32, &Mutex<PgConnection>)> {
        self.unlocked_con
            .pop_front()
            .and_then(|idx| self.inner.get(idx.value()).map(|conn| (*idx.value(), conn)))
    }

    fn put_back_conn(&self, id: i32) {
        self.unlocked_con.insert(id);
    }

    fn keep_alive(
        connections: Arc<BTreeMap<i32, Mutex<PgConnection>>>,
        datasource: &str,
        rx: oneshot::Receiver<()>,
    ) {
        tokio::spawn(DieselKeepAliveHelper::new(connections, datasource.to_string()).start(rx));
    }
}

fn handle_instrumentation_event(event: InstrumentationEvent<'_>) {
    match event {
        InstrumentationEvent::StartEstablishConnection { url, .. } => {
            tracing::info!("start establish connection: {}", url);
        }
        InstrumentationEvent::FinishEstablishConnection { url, error, .. } => {
            error.iter().for_each(|e| {
                tracing::error!("finish establish connection: {} error: {}", url, e);
            });
        }
        InstrumentationEvent::StartQuery { query, .. } => {
            tracing::debug!("execution_plan query: {}", query);
        }
        InstrumentationEvent::CacheQuery { sql, .. } => {
            tracing::debug!("cached execution_plan query: {}", sql);
        }
        InstrumentationEvent::FinishQuery { error, .. } => {
            error.iter().for_each(|e| {
                tracing::error!("finish execution_plan query failed: {}", e);
            });
        }
        InstrumentationEvent::BeginTransaction { .. } => {}
        InstrumentationEvent::CommitTransaction { .. } => {}
        InstrumentationEvent::RollbackTransaction { .. } => {}
        _ => {}
    }
}

#[derive(new)]
struct DieselKeepAliveHelper {
    connections: Arc<BTreeMap<i32, Mutex<PgConnection>>>,
    datasource: String,
}

impl DieselKeepAliveHelper {
    async fn start(self, mut rx: oneshot::Receiver<()>) {
        use diesel::RunQueryDsl;
        use std::ops::ControlFlow;
        let mut interval = tokio::time::interval(Duration::from_secs(120));
        tokio::spawn(async move {
            loop {
                let flow = tokio::select! {
                    _ = &mut rx => {
                        ControlFlow::Break(())
                    }
                    _ = interval.tick() => {
                        ControlFlow::Continue(())
                    }
                };

                match flow {
                    ControlFlow::Continue(_) => {
                        self.connections.iter().for_each(|(_, conn)| {
                            let mut conn = mutex(conn);
                            if let Err(e) = diesel::sql_query("SELECT 1").execute(&mut *conn) {
                                tracing::error!("diesel keepalive check failed: {}", e);
                                *conn = Pool::new_connection(&self.datasource);
                            }
                        });
                    }
                    ControlFlow::Break(_) => break,
                }
            }
        });
    }
}
