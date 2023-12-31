use std::{ffi::OsStr, net::ToSocketAddrs, time::Duration};

use tokio::{io, net::lookup_host};

const BASE_BACKOFF_TIME: Duration = Duration::from_millis(300);
pub const MAX_BACKOFF_TIMES: u32 = 3;

pub fn get_env<P: AsRef<OsStr>>(key: P) -> Option<String> {
    std::env::var(key).ok()
}

pub fn new_trace_id() -> String {
    uuid::Uuid::new_v4().to_string().replace("-", "")
}

pub fn hostname() -> Option<String> {
    hostname::get()
        .map_err(|err| {
            tracing::error!("get hostname failed: {}", &err);
            err
        })
        .ok()
        .and_then(|val| val.to_str().map(|val| val.to_string()))
}

pub async fn look_up(dst: String) -> Result<Option<std::net::SocketAddr>, io::Error> {
    Ok(match lookup_host(dst).await?.next() {
        Some(addr) => addr.to_socket_addrs()?.next(),
        None => None,
    })
}

pub mod conf {
    use std::io;

    use regex::Regex;
    use serde::de;

    use super::get_env;

    pub fn from_yml_reader<R, T>(mut rdr: R) -> io::Result<T>
    where
        R: io::Read,
        T: de::DeserializeOwned,
    {
        let buf = &mut String::new();
        rdr.read_to_string(buf).and_then(|_| {
            let replaced = replace_by_env(&buf);
            serde_yaml::from_str::<T>(&replaced)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        })
    }

    pub(crate) fn replace_by_env(origin: &str) -> String {
        const REGEX: &'static str = r"\$\{[^}]+\}";
        let r = Regex::new(REGEX).expect("msg");
        let mut new_string = origin.to_string();
        r.captures_iter(origin).for_each(|captures| {
            captures.iter().for_each(|cap| {
                cap.iter().for_each(|matcher| {
                    let placeholder = matcher.as_str();
                    let placeholder = &placeholder[2..placeholder.len() - 1];

                    get_env(placeholder).into_iter().for_each(|env_val| {
                        new_string = new_string.replace(matcher.as_str(), env_val.as_str());
                    })
                });
            });
        });
        new_string
    }

    pub async fn load_config<T>() -> io::Result<T>
    where
        T: de::DeserializeOwned,
    {
        let path = get_env("MPS_CONFIG_PATH").unwrap_or("etc/config.yaml".to_string());
        match tokio::fs::OpenOptions::new().read(true).open(path).await {
            Ok(file) => from_yml_reader(file.into_std().await)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err)),
            Err(err) => Err(err),
        }
    }
}

pub mod codec {
    use crate::lang::vec::index_for_each;

    macro_rules! bcd_to_int {
        ($bits:ident, $len:expr, $name:ident) => {
            pub fn $name(buf: &[u8; $len]) -> $bits {
                match $bits::from_str_radix(
                    format!("{:x}", bytes::Bytes::copy_from_slice(buf)).as_str(),
                    10,
                ) {
                    Ok(val) => val,
                    Err(err) => {
                        tracing::error!("bcd to uint failed: {}", err);
                        0
                    }
                }
            }
        };
    }

    macro_rules! hex_to_int {
        ($bits:ident, $len:expr, $name:ident) => {
            pub fn $name(buf: &[u8; $len]) -> $bits {
                $bits::from_le_bytes(*buf)
            }
        };
    }

    macro_rules! int_to_hex {
        ($bits:ident, $len:expr, $name:ident) => {
            pub fn $name(val: $bits) -> [u8; $len] {
                val.to_le_bytes()
            }
        };
    }

    bcd_to_int!(u32, 4, bcd_to_u32);
    bcd_to_int!(u64, 8, bcd_to_u64);
    bcd_to_int!(i32, 4, bcd_to_i32);
    bcd_to_int!(u16, 2, bcd_to_u16);

    hex_to_int!(u16, 2, hex_to_u16);
    hex_to_int!(u32, 4, hex_to_u32);
    hex_to_int!(i32, 4, hex_to_i32);
    hex_to_int!(u64, 8, hex_to_u64);

    int_to_hex!(u64, 8, u64_to_hex);
    int_to_hex!(u16, 2, u16_to_hex);
    int_to_hex!(u32, 4, u32_to_hex);

    pub fn u64_to_bcd(val: u64) -> [u8; 8] {
        let mut v = val.to_string();
        let mut size = v.len();
        if size > 16 {
            size = 16;
        }

        if size % 2 != 0 {
            v.insert(0, '0');
            size += 1;
        }

        let mut r = vec![];
        (0..size).step_by(2).for_each(|idx| {
            r.push(match u8::from_str_radix(&v[idx..idx + 2], 16) {
                Ok(b) => b,
                Err(err) => {
                    tracing::error!("parse hex to u8 failed: {}", err);
                    0
                }
            });
        });

        let mut result = [0u8; 8];
        r.reverse();
        index_for_each(&r, |idx, b| {
            result[7 - idx] = *b;
        });

        result
    }

    pub fn hex_string_as_slice(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|idx| match u8::from_str_radix(&text[idx..idx + 2], 16) {
                Ok(b) => b,
                Err(err) => {
                    tracing::error!("invalid parse hex string: {}", err);
                    0
                }
            })
            .collect()
    }

    pub fn u16_to_bcd(value: u16) -> [u8; 2] {
        let mut v = value.to_string();
        let mut size = v.len();
        if size > 4 {
            size = 4;
        }

        (0..4 - size).for_each(|_| v.insert(0, '0'));

        let mut result = [0u8; 2];
        result[1] = match u8::from_str_radix(&v[2..4], 16) {
            Ok(b) => b,
            Err(err) => {
                tracing::error!("parse byte failed: {}", err);
                0
            }
        };
        result[0] = match u8::from_str_radix(&v[0..2], 16) {
            Ok(b) => b,
            Err(err) => {
                tracing::error!("parse byte failed: {}", err);
                0
            }
        };

        result
    }
}

pub fn exponential_backoff(times: u32) -> Duration {
    let mut backoff = BASE_BACKOFF_TIME;
    backoff *= 1 << times;
    backoff
}

// pub fn consistant_hash<'a, K: Hash, N: Hash>(key: &'a K, nodes: &'a [N]) -> &'a N {
//     let mut hasher = DefaultHasher::new();
//     key.hash(&mut hasher);
//     let hash = hasher.finish();

//     let mut hash_group: BTreeMap<u64, &N> = Default::default();

//     let mut node_hashes: Vec<u64> = nodes
//         .iter()
//         .map(|node| {
//             let mut hasher = DefaultHasher::new();
//             node.hash(&mut hasher);
//             let r = hasher.finish();
//             hash_group.insert(r, node);
//             r
//         })
//         .collect();
//     node_hashes.sort();

//     for node in node_hashes {
//         if hash <= node {
//             return hash_group.get(&node).unwrap();
//         }
//     }

//     &nodes[0]
// }

#[cfg(test)]
mod tests {
    use std::env;

    use crate::utils::codec::{
        bcd_to_i32, bcd_to_u16, bcd_to_u32, bcd_to_u64, hex_to_i32, hex_to_u32, hex_to_u64,
        u64_to_hex,
    };

    use super::{
        codec::{hex_string_as_slice, hex_to_u16, u16_to_bcd, u64_to_bcd},
        conf::replace_by_env,
        hostname,
    };

    #[test]
    fn test_hex_string_to_slice() {
        let expected: &[u8] = &[
            0xA0, 0x5A, 0x93, 0x83, 0x46, 0xB4, 0x56, 0xB8, 0xE2, 0x55, 0x4E, 0xFA, 0x33, 0x55,
            0x91, 0x57,
        ];

        assert_eq!(
            expected,
            &hex_string_as_slice("a05a938346b456b8e2554efa33559157")
        )
    }

    #[test]
    fn test_replace_by_env() {
        let input = "{\"your_name\": \"${name}\", \"your_id\": ${id}, \"your_home\": \"${home}\"}";
        env::set_var("name", "jason");
        env::set_var("id", "1111");
        env::set_var("home", "shanghai");
        let result = replace_by_env(input);

        assert_eq!(
            result.as_str(),
            "{\"your_name\": \"jason\", \"your_id\": 1111, \"your_home\": \"shanghai\"}"
        )
    }

    #[test]
    fn test_hex_to_u16() {
        let r = hex_to_u16(&[0x3c, 0x06]);
        assert_eq!(r, 0x063c);
    }

    #[test]
    fn test_bcd_to_u16() {
        let r = bcd_to_u16(&[0x12, 0x06]);
        assert_eq!(r, 1206);
    }

    #[test]
    fn test_hex_to_i32() {
        let r = hex_to_i32(&[0x3c, 0x06, 0x3b, 0xff]);
        let expected =
            (0xff & 0xff) << 24 | (0x3b & 0xff) << 16 | (0x06 & 0xff) << 8 | (0x3c & 0xff) << 0;
        println!("expected: {}, actual: {}", expected, r);
        assert_eq!(r, expected);
    }

    #[test]
    fn test_bcd_to_i32() {
        let r = bcd_to_i32(&[0x30, 0x06, 0x37, 0x13]);
        assert_eq!(r, 30063713);
    }

    #[test]
    fn test_bcd_to_u32() {
        let r = bcd_to_u32(&[0x10, 0x06, 0x30, 0x10]);
        assert_eq!(r, 10063010);
    }
    #[test]
    fn test_hex_to_u32() {
        let r = hex_to_u32(&[0x3c, 0x06, 0x3b, 0xff]);
        let expected =
            (0xff & 0xff) << 24 | (0x3b & 0xff) << 16 | (0x06 & 0xff) << 8 | (0x3c & 0xff) << 0;
        println!("expected: {}, actual: {}", expected, r);
        assert_eq!(r, expected);
    }

    #[test]
    fn test_hex_to_u64() {
        let r = hex_to_u64(&[0x3c, 0x06, 0x3b, 0xff, 0x3c, 0x06, 0x3b, 0xff]);
        let expected = (0xff & 0xff) << 56
            | (0x3b & 0xff) << 48
            | (0x06 & 0xff) << 40
            | (0x3c & 0xff) << 32
            | (0xff & 0xff) << 24
            | (0x3b & 0xff) << 16
            | (0x06 & 0xff) << 8
            | (0x3c & 0xff) << 0;
        println!("expected: {}, actual: {}", expected, r);
        assert_eq!(r, expected);
    }

    #[test]
    fn test_bcd_to_u64() {
        let r = bcd_to_u64(&[0x20, 0x23, 0x03, 0x00, 0x13, 0x06, 0x36, 0x99]);
        let expected = 2023030013063699;
        assert_eq!(r, expected);
    }

    #[test]
    fn test_hostname() {
        let hostname = hostname();
        assert!(hostname.is_some());

        println!("{}", hostname.unwrap_or_default());
    }

    #[test]
    fn test_u16_to_bcd() {
        {
            let r = u16_to_bcd(1010 as u16);
            assert_eq!(r, [0x10, 0x10])
        }

        {
            let r = u16_to_bcd(10 as u16);
            assert_eq!(r, [0x00, 0x10])
        }
    }

    #[test]
    fn test_u64_to_bcd() {
        let r = u64_to_bcd(20230305 as u64);
        assert_eq!(r, [0x00, 0x00, 0x00, 0x00, 0x20, 0x23, 0x03, 0x05])
    }

    #[test]
    fn test_u64_to_hex() {
        let r = u64_to_hex(255 as u64);
        assert_eq!(r, [0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    }
}