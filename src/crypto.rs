use aes::{
    cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher, StreamCipherError},
    Aes128,
};
use std::fmt::{Debug, Display};

use crate::utils::codec::hex_string_as_slice;

#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum Cryption {
    Unspecified,
    Aes128Ctr { key: String, iv: String },
}
impl Cryption {
    pub fn decrypt<'a>(&'a self, body_buf: &'a [u8]) -> Result<bytes::Bytes, CryptionError> {
        match self {
            Cryption::Aes128Ctr { key, iv } => {
                let key = hex_string_as_slice(key.as_str());
                let iv = hex_string_as_slice(iv.as_str());
                let mut cipher = ctr::Ctr128BE::<Aes128>::new(
                    &GenericArray::from_iter(key),
                    &GenericArray::from_iter(iv),
                );

                let mut output = vec![0u8; body_buf.len()];
                cipher
                    .apply_keystream_b2b(body_buf, &mut output)
                    .map(|_| bytes::Bytes::from_iter(output))
                    .map_err(|err| CryptionError::SymmetricCipherError(err))
            }
            _ => Ok(bytes::Bytes::copy_from_slice(body_buf)),
        }
    }

    pub fn encrypt(&self, buf: &[u8]) -> Result<bytes::Bytes, CryptionError> {
        match self {
            Cryption::Unspecified => Ok(bytes::Bytes::copy_from_slice(buf)),
            Cryption::Aes128Ctr { key, iv } => {
                let key = hex_string_as_slice(key.as_str());
                let iv = hex_string_as_slice(iv.as_str());
                let mut cipher = ctr::Ctr128BE::<Aes128>::new(
                    &GenericArray::from_iter(key),
                    &GenericArray::from_iter(iv),
                );

                let mut output = vec![0u8; buf.len()];
                cipher
                    .apply_keystream_b2b(buf, &mut output)
                    .map(|_| bytes::Bytes::from_iter(output))
                    .map_err(|err| CryptionError::SymmetricCipherError(err))
            }
        }
    }
}

#[derive(Debug)]
pub enum CryptionError {
    SymmetricCipherError(StreamCipherError),
    InvalidLength(aes::cipher::InvalidLength),
}

impl Display for CryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptionError::SymmetricCipherError(err) => std::fmt::Display::fmt(&err, f),
            CryptionError::InvalidLength(err) => std::fmt::Display::fmt(&err, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cryption;

    #[test]
    fn test_cryption_decrypt() {
        let cryption = Cryption::Aes128Ctr {
            key: "11111111111111111111111111111111".to_string(),
            iv: "0102030405060708090a0b0c0d0e0f10".to_string(),
        };
        let r = cryption.encrypt("123456789abcdefghijk".as_bytes());
        assert!(r.is_ok());

        let encrypt_data = r.expect("msg");

        let r = cryption.decrypt(&encrypt_data);
        assert!(r.is_ok());
        let decrypt_data = r.expect("msg");
        assert_eq!(&decrypt_data, "123456789abcdefghijk".as_bytes());
    }
}
