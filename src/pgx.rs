use std::{str::FromStr, sync::Arc, time::Duration};
use std::collections::BTreeMap;
use std::sync::Mutex;

use crossbeam_skiplist::SkipSet;
use diesel::{Connection, ConnectionResult, PgConnection};
use diesel::backend::Backend;
use diesel::connection::{Instrumentation, LoadConnection, SimpleConnection, TransactionManager};
use diesel::migration::{MigrationSource, MigrationVersion};
use diesel::query_builder::{QueryFragment, QueryId};
use postgres_types::ToSql;
use tokio::sync::RwLock;
use tokio_postgres::{Config, NoTls, Row, Socket, tls::NoTlsStream};

use crate::concurrency::mutex;
use crate::utils::num_cpus;

#[derive(Clone)]
pub struct Pool {
    inner: Arc<BTreeMap<i32, Mutex<PgConnection>>>,
    unlocked_con: Arc<SkipSet<i32>>,
}

impl Default for Pool {
    fn default() -> Self {
        let datasource = env!("DATABASE_URL");
        let mut active_num = num_cpus();
        if active_num == 0 {
            active_num = 1;
        }

        Self::new(datasource, active_num)
    }
}

macro_rules! pool_load_dsl {
    ($name:ident,$ret_val:expr) => {
        impl Pool {
            pub fn $name<'query, U, DSL: diesel::RunQueryDsl<PgConnection>>(
                &self,
                dsl: DSL,
            ) -> diesel::QueryResult<$ret_val>
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
                        Box::new("no active connection"),
                    )))
            }
        }
    };
}

impl Pool {
    pub fn new(datasource: &str, active_num: usize) -> Self {
        let mut inner = BTreeMap::default();
        let unlocked_con = SkipSet::from_iter(0i32..(active_num as i32));
        (0..active_num).for_each(|idx| {
            inner.insert(
                (idx + 1) as i32,
                Mutex::new(PgConnection::establish(datasource).expect("connect pg failed")),
            );
        });

        Self {
            inner: Arc::new(inner),
            unlocked_con: Arc::new(unlocked_con),
        }
    }

    pub fn run_pending_migrations<S: MigrationSource<DB>, DB>(
        &self,
        source: S,
    ) -> diesel::migration::Result<Vec<MigrationVersion>>
    where
        DB: Backend,
    {
        use diesel_migrations::MigrationHarness;
        let (id, conn) = self.get_active_conn().expect("no active connection");

        let conn = &mut *mutex(conn);
        let result = conn.run_pending_migrations(source);

        self.put_back_conn(id);
        result
    }
}

pool_load_dsl!(load, Vec<U>);
pool_load_dsl!(get_result, U);

impl Pool {
    fn get_active_conn(&self) -> Option<(i32, &Mutex<PgConnection>)> {
        self.unlocked_con
            .pop_front()
            .and_then(|idx| self.inner.get(idx.value()).map(|conn| (*idx.value(), conn)))
    }

    fn put_back_conn(&self, id: i32) {
        self.unlocked_con.insert(id);
    }
}

#[derive(Clone)]
pub struct Postgres {
    cli: Arc<RwLock<tokio_postgres::Client>>,
}

#[derive(Clone, serde::Deserialize, Debug)]
pub struct PostgresBuilder {
    pub hostname: String,
    pub port: u16,
    pub db: String,
    pub username: String,
    pub password: String,
    pub connect_timeout: u64,
}

impl Default for PostgresBuilder {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 5432,
            db: "postgres".to_string(),
            username: "postgres".to_string(),
            password: "postgres".to_string(),
            connect_timeout: 3000,
        }
    }
}

impl PostgresBuilder {
    pub async fn parse(datasource: &str) -> Postgres {
        let conf = Config::from_str(datasource).expect("parse pg datasource failed");
        Self::create(conf).await
    }

    pub async fn build(&self) -> Postgres {
        let conf = self.as_conf();
        Self::create(conf).await
    }

    fn as_conf(&self) -> Config {
        Config::new()
            .dbname(self.db.as_str())
            .host(&self.hostname)
            .password(&self.password)
            .port(self.port)
            .application_name("mps")
            .user(&self.username)
            .connect_timeout(Duration::from_millis(self.connect_timeout))
            .keepalives_interval(Duration::from_secs(60))
            .keepalives_retries(3)
            .to_owned()
    }

    async fn create(conf: Config) -> Postgres {
        let (client, connection) = conf
            .connect(NoTls)
            .await
            .expect("create pg connection failed");
        monitor_connection(connection);

        let cli = Arc::new(RwLock::new(client));

        let cli_cloned = cli.clone();

        tokio::spawn(async move {
            let checker = KeepAliveChecker {
                client: cli_cloned,
                conf,
            };
            checker.start().await
        });

        Postgres { cli }
    }
}

impl Postgres {
    pub async fn query<'a, T: Table>(
        &'a self,
        stmt: &'a str,
        params: &'a [&(dyn ToSql + Sync)],
    ) -> Result<Vec<T>, tokio_postgres::Error> {
        let stmt = self.cli.read().await.prepare(stmt).await?;
        self.cli
            .read()
            .await
            .query(&stmt, params)
            .await
            .map(|row| row.iter().map(T::from_row).collect())
    }

    pub async fn execute(
        &self,
        stmt: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::Error> {
        let stmt = self.cli.read().await.prepare(stmt).await?;
        self.cli.read().await.execute(&stmt, params).await
    }

    pub async fn query_one<'a, T: Table>(
        &'a self,
        stmt: &'a str,
        params: &'a [&(dyn ToSql + Sync)],
    ) -> Result<T, tokio_postgres::Error> {
        let stmt = self.cli.read().await.prepare(stmt).await?;

        self.cli
            .read()
            .await
            .query_one(&stmt, params)
            .await
            .map(|row| T::from_row(&row))
    }

    pub async fn query_opt<'a, T: Table>(
        &'a self,
        stmt: &'a str,
        params: &'a [&(dyn ToSql + Sync)],
    ) -> Result<Option<T>, tokio_postgres::Error> {
        let guard = self.cli.read().await;
        let stmt = guard.prepare(stmt).await?;
        guard
            .query_opt(&stmt, params)
            .await
            .map(|row| row.map(|r| T::from_row(&r)))
    }
}

pub fn prepare_insert_stmt<T: Table>() -> String {
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        T::table(),
        T::columns().join(","),
        T::columns()
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect::<Vec<String>>()
            .join(",")
    )
}

pub trait Table {
    fn table() -> &'static str;
    fn columns() -> &'static [&'static str];
    fn from_row(row: &Row) -> Self;
}

struct KeepAliveChecker {
    client: Arc<RwLock<tokio_postgres::Client>>,
    conf: Config,
}

impl KeepAliveChecker {
    async fn start(self) {
        let mut interval = tokio::time::interval(
            self.conf
                .get_keepalives_interval()
                .unwrap_or(Duration::from_secs(60)),
        );

        loop {
            interval.tick().await;
            let client = self.client.read().await;
            if let Err(e) = client.execute("SELECT 1", &[]).await {
                tracing::error!("pg keepalive check failed: {}", e);

                drop(client);

                if let Ok((client, conn)) = self.conf.connect(NoTls).await {
                    let mut client_mut = self.client.write().await;
                    *client_mut = client;
                    drop(client_mut);
                    monitor_connection(conn)
                }
            }
        }
    }
}

fn monitor_connection(conn: tokio_postgres::Connection<Socket, NoTlsStream>) {
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            tracing::error!("pg reconnect error: {}", err);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;
    use tokio_postgres::Row;

    use crate::pgx::PostgresBuilder;

    use super::Table;

    struct Person {
        id_card: String,
        name: String,
        address: Option<String>,
        country: Option<String>,
    }

    impl Table for Person {
        fn table() -> &'static str {
            "person"
        }

        fn columns() -> &'static [&'static str] {
            &["id_card", "name", "address", "country"]
        }

        fn from_row(row: &Row) -> Self {
            Self {
                id_card: row.get(0),
                name: row.get(1),
                address: row.get(2),
                country: row.get(3),
            }
        }
    }

    #[tokio::test]
    async fn test_query() {
        let mut builder = PostgresBuilder::default();
        builder.db = "rxdomain".to_string();
        let create_db = "CREATE TABLE person
        (
            id bigserial NOT NULL,
            name character varying NOT NULL,
            id_card character varying NOT NULL,
            address character varying,
            country character varying,
            PRIMARY KEY (id)
        );";

        let pg = builder.build().await;
        let _ = pg.execute(create_db, &[]).await;

        let insert = format!(
            "INSERT INTO {} ({}) VALUES ($1, $2, $3, $4)",
            Person::table(),
            Person::columns().join(",")
        );

        let _ = pg
            .execute(insert.as_str(), &[&"11111111", &"test", &"123", &"china"])
            .await;

        let _ = pg
            .execute(insert.as_str(), &[&"22222222", &"test", &"123", &"china"])
            .await;

        let rows = pg
            .query::<Person>(
                &format!(
                    "SELECT {} FROM {} WHERE id_card = $1",
                    Person::columns().join(","),
                    Person::table()
                ),
                &[&"11111111"],
            )
            .await
            .expect("msg");

        assert_eq!(rows.len(), 1);

        let person = rows.get(0).expect("msg");

        assert_eq!(person.id_card, "11111111");
        assert_eq!(person.name, "test");
        assert_eq!(person.address, Some("123".to_string()));
        assert_eq!(person.country, Some("china".to_string()));

        let _ = pg
            .execute(&format!("DROP TABLE {}", Person::table()), &[])
            .await;
    }

    #[tokio::test]
    async fn test_rwlock_replace() {
        let val = Arc::new(RwLock::new(1));

        let val1 = val.clone();
        let val2 = val.clone();
        *val1.write().await = 2;
        let handler = tokio::spawn(async move {
            assert_eq!(2, *val2.read().await);
        });
        assert_eq!(2, *val.read().await);
        handler.await;
    }
}
