use std::{fmt::Display, path::Path, sync::Arc};

static GB_SIZE: u64 = 1 << 30;
static MB_SIZE: u64 = 1 << 20;
static KB_SIZE: u64 = 1 << 10;

#[derive(Debug, serde::Deserialize, Clone)]
#[serde(tag = "type", content = "value")]
pub enum CacheSize {
    Gb(u32),
    Mb(u64),
    Kb(u64),
}
impl CacheSize {
    fn to_byte(&self) -> u64 {
        match self {
            CacheSize::Gb(size) => *size as u64 * GB_SIZE,
            CacheSize::Mb(size) => *size as u64 * MB_SIZE,
            CacheSize::Kb(size) => *size as u64 * KB_SIZE,
        }
    }
}

pub trait Kv: Clone + Send + Sync {
    fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<bytes::Bytes>, DBError>;

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError>;

    fn delete<K: AsRef<[u8]>>(&self, key: &K) -> Result<(), DBError>;

    fn list<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        if keys.len() == 1 {
            return self.get(&keys[0]).map(|v| {
                v.map(|v| vec![(keys[0].as_ref().to_vec(), v)])
                    .unwrap_or_default()
            });
        }
        let mut keys = keys
            .iter()
            .filter(|key| self.maybe_contains(*key).unwrap_or_default())
            .map(|k| k.as_ref().to_vec())
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();

        self.list_keys(keys)
    }

    fn list_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<Vec<bytes::Bytes>, DBError>;

    fn maybe_contains<K: AsRef<[u8]>>(&self, key: &K) -> Result<bool, DBError>;

    fn list_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError>;
}

#[cfg(feature = "rocksdb-enable")]
#[derive(Clone)]
pub struct RocksKv {
    db: Arc<rocksdb::DB>,
}

#[cfg(feature = "sled-enable")]
#[derive(Clone)]
pub struct SledKv {
    db: sled::Db,
}

#[cfg(feature = "rocksdb-enable")]
impl RocksKv {
    fn cf_handle(&self, cf: &str) -> Option<&rocksdb::ColumnFamily> {
        self.db.cf_handle(cf.as_ref())
    }

    fn get_cf<K: AsRef<[u8]>>(&self, cf: &str, key: K) -> Result<Option<bytes::Bytes>, DBError> {
        match self.cf_handle(cf) {
            Some(cf) => self
                .db
                .get_cf(cf, key)
                .map(|v| v.map(|v| bytes::Bytes::copy_from_slice(&v)))
                .map_err(|err| err.into()),
            None => Ok(None),
        }
    }

    fn delete_cf<K: AsRef<[u8]>>(&self, cf: &str, key: K) -> Result<(), DBError> {
        match self.cf_handle(cf) {
            Some(cf) => self
                .db
                .delete_cf(cf, key)
                .map(|_| {})
                .map_err(|err| err.into()),
            None => Ok(()),
        }
    }

    fn list_prefix_cf<K: AsRef<[u8]>>(
        &self,
        cf: &str,
        prefix: K,
    ) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError> {
        match self.cf_handle(cf) {
            Some(cf) => self
                .db
                .prefix_iterator_cf(cf, prefix)
                .map(|result| {
                    result
                        .map(|(k, v)| ((*k).to_vec(), bytes::Bytes::copy_from_slice(&v)))
                        .map_err(|err| err.into())
                })
                .collect(),
            None => Ok(vec![]),
        }
    }

    fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &str,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        match self.cf_handle(cf) {
            Some(cf) => self
                .db
                .put_cf(cf, key, value)
                .map(|_| None)
                .map_err(|err| err.into()),
            None => Ok(None),
        }
    }
}

#[cfg(feature = "rocksdb-enable")]
impl Kv for RocksKv {
    fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<bytes::Bytes>, DBError> {
        self.db
            .get(key)
            .map(|v| v.map(|v| bytes::Bytes::copy_from_slice(&v)))
            .map_err(|err| err.into())
    }

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        self.db
            .put(key, value)
            .map(|_| None)
            .map_err(|err| err.into())
    }

    fn delete<K: AsRef<[u8]>>(&self, key: &K) -> Result<(), DBError> {
        self.db.delete(key).map_err(|err| err.into())
    }

    fn list_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError> {
        let mut group_errs = vec![];

        let mut opt = rocksdb::ReadOptions::default();
        opt.set_iterate_range((keys[0].clone())..(keys[keys.len() - 1].clone()));
        opt.set_prefix_same_as_start(true);

        let results = self
            .db
            .iterator_opt(
                rocksdb::IteratorMode::From(keys[0].as_ref(), rocksdb::Direction::Forward),
                opt,
            )
            .filter(|r| match r {
                Ok((key, _)) => keys.binary_search(&(*key).to_vec()).is_ok(),
                Err(_) => true,
            })
            .map(|result| {
                result
                    .map(|(key, v)| ((*key).to_vec(), bytes::Bytes::copy_from_slice(&*v)))
                    .map_err(|err| group_errs.push(err.into()))
            })
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();

        if group_errs.is_empty() {
            Ok(results)
        } else {
            Err(DBError::Group(group_errs))
        }
    }

    fn list_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<Vec<bytes::Bytes>, DBError> {
        let mut group_errs = vec![];
        let results = self
            .db
            .prefix_iterator(prefix)
            .map(|result| {
                result
                    .map(|(_, v)| bytes::Bytes::copy_from_slice(&*v))
                    .map_err(|err| group_errs.push(err.into()))
            })
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();

        if group_errs.is_empty() {
            Ok(results)
        } else {
            Err(DBError::Group(group_errs))
        }
    }

    fn maybe_contains<K: AsRef<[u8]>>(&self, key: &K) -> Result<bool, DBError> {
        Ok(self.db.key_may_exist(key))
    }
}

#[cfg(feature = "sled-enable")]
impl Kv for SledKv {
    fn get<T: AsRef<[u8]>>(&self, key: &T) -> Result<Option<bytes::Bytes>, DBError> {
        self.db
            .get(key)
            .map(|v| v.map(|v| bytes::Bytes::copy_from_slice(&v)))
            .map_err(|err| err.into())
    }

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        self.db
            .insert(key, value.as_ref())
            .map(|v| v.map(|v| bytes::Bytes::copy_from_slice(&v)))
            .map_err(|err| err.into())
    }

    fn delete<K: AsRef<[u8]>>(&self, key: &K) -> Result<(), DBError> {
        self.db.remove(key).map(|_| {}).map_err(|err| err.into())
    }

    fn list_keys(&self, keys: Vec<Vec<u8>>) -> Result<(Vec<u8>, bytes::Bytes), DBError> {
        let mut group_errs = vec![];
        let result = self
            .db
            .range(keys[0].clone()..keys[keys.len() - 1].clone())
            .map(|r| {
                r.map(|(key, v)| (key.to_vec(), bytes::Bytes::copy_from_slice(&v)))
                    .map_err(|err| group_errs.push(err.into()))
            })
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();

        if group_errs.is_empty() {
            Ok(result)
        } else {
            Err(DBError::Group(group_errs))
        }
    }

    fn list_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<(Vec<u8>, bytes::Bytes), DBError> {
        let mut group_errs = vec![];

        let results = self
            .db
            .scan_prefix(prefix)
            .map(|r| {
                r.map(|(key, v)| (key.to_vec(), bytes::Bytes::copy_from_slice(&v)))
                    .map_err(|err| group_errs.push(err.into()))
            })
            .filter(|r| r.is_ok())
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();

        if group_errs.is_empty() {
            Ok(results)
        } else {
            Err(DBError::Group(group_errs))
        }
    }

    fn maybe_contains<K: AsRef<[u8]>>(&self, key: &K) -> Result<bool, DBError> {
        self.db.contains_key(&key).map_err(|err| err.into())
    }
}

#[cfg(feature = "rocksdb-enable")]
pub type DB = DBWithInnerKvStore<RocksKv>;

#[cfg(feature = "sled-enable")]
pub type DB = DBWithInnerKvStore<SledKv>;

#[cfg(feature = "rocksdb-enable")]
#[derive(Default)]
pub struct Options {
    pub cache_size: Option<CacheSize>,
    pub prefix: Option<usize>,
}
impl Options {
    pub fn fixed_prefix_length(&mut self, prefix: usize) {
        self.prefix = Some(prefix);
    }
}

#[derive(Clone)]
pub struct DBWithInnerKvStore<Inner: Kv> {
    db: Inner,
}

#[cfg(feature = "rocksdb-enable")]
impl DBWithInnerKvStore<RocksKv> {
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<Self, DBError> {
        rocksdb::DB::open_default(path)
            .map(|db| Self {
                db: RocksKv { db: Arc::new(db) },
            })
            .map_err(|err| err.into())
    }

    pub fn open<P: AsRef<Path>>(options: &Options, path: P) -> Result<Self, DBError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        if let Some(prefix_length) = options.prefix {
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(prefix_length));
        }
        rocksdb::DB::open(&opts, path)
            .map(|db| Self {
                db: RocksKv { db: Arc::new(db) },
            })
            .map_err(|err| err.into())
    }

    pub fn open_cf<P, I, N>(options: &Options, path: P, cfs: I) -> Result<Self, DBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        if let Some(prefix_length) = options.prefix {
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(prefix_length));
        }
        rocksdb::DB::open_cf(&opts, path, cfs)
            .map(|db| Self {
                db: RocksKv { db: Arc::new(db) },
            })
            .map_err(|err| err.into())
    }

    pub fn open_cf_default<P, I, N>(path: P, cfs: I) -> Result<Self, DBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        rocksdb::DB::open_cf(&opts, path, cfs)
            .map(|db| Self {
                db: RocksKv { db: Arc::new(db) },
            })
            .map_err(|err| err.into())
    }

    pub fn get_cf<K: AsRef<[u8]>>(
        &self,
        cf: &str,
        key: K,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        self.db.get_cf(cf, key)
    }

    pub fn delete_cf<K: AsRef<[u8]>>(&self, cf: &str, key: K) -> Result<(), DBError> {
        self.db.delete_cf(cf, key)
    }

    pub fn list_prefix_cf<K: AsRef<[u8]>>(
        &self,
        cf: &str,
        prefix: K,
    ) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError> {
        self.db.list_prefix_cf(cf, prefix)
    }

    pub fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf: &str,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        self.db.put_cf(cf, key, value)
    }
}

#[cfg(feature = "sled-enable")]
impl DBWithInnerKvStore<SledKv> {
    pub fn open(path: &str) -> Self {
        let db = sled::open(path).expect("open sled failed");
        Self { db: SledKv { db } }
    }
}

impl<Inner: Kv> DBWithInnerKvStore<Inner> {
    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<bytes::Bytes>, DBError> {
        self.db.get(key)
    }

    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<bytes::Bytes>, DBError> {
        self.db.put(key, value)
    }

    pub fn delete<K: AsRef<[u8]>>(&self, key: &K) -> Result<(), DBError> {
        self.db.delete(key)
    }

    pub fn list<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<Vec<(Vec<u8>, bytes::Bytes)>, DBError> {
        self.db.list(keys)
    }

    pub fn list_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<Vec<bytes::Bytes>, DBError> {
        self.db.list_prefix(prefix)
    }
}

#[derive(Debug)]
pub enum DBError {
    NotFound,
    Other(String),
    IoError(String),
    InvalidArgument(String),
    Unknown(String),
    Group(Vec<DBError>),
}

impl Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::NotFound => write!(f, "not found"),
            DBError::Other(err) => write!(f, "other error: {}", err),
            DBError::IoError(err) => write!(f, "io error: {}", err),
            DBError::InvalidArgument(err) => write!(f, "invalid argument: {}", err),
            DBError::Unknown(err) => write!(f, "unknown error: {}", err),
            DBError::Group(errs) => {
                write!(f, "group error: [")?;
                for err in errs {
                    write!(f, "{}, ", err)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(feature = "rocksdb-enable")]
impl From<rocksdb::Error> for DBError {
    fn from(err: rocksdb::Error) -> Self {
        match err.kind() {
            rocksdb::ErrorKind::NotFound => DBError::NotFound,
            rocksdb::ErrorKind::InvalidArgument => DBError::InvalidArgument(err.to_string()),
            rocksdb::ErrorKind::IOError => DBError::IoError(err.to_string()),
            rocksdb::ErrorKind::Unknown => DBError::Unknown(err.to_string()),
            _ => DBError::Other(err.to_string()),
        }
    }
}

#[cfg(feature = "sled-enable")]
impl From<sled::Error> for DBError {
    fn from(err: sled::Error) -> Self {
        match err {
            sled::Error::Io(err) => DBError::IoError(err.to_string()),
            sled::Error::CollectionNotFound(_) => DBError::NotFound,
            _ => DBError::Other(err.to_string()),
        }
    }
}
