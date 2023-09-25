use std::fmt::{Debug, Display};

use crypto::{
    aes::KeySize,
    buffer::{BufferResult, ReadBuffer, RefReadBuffer, RefWriteBuffer, WriteBuffer},
    symmetriccipher::{Decryptor, Encryptor, SymmetricCipherError},
};

use crate::utils::codec::hex_string_as_slice;
extern crate crypto;

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
                let mut ciphter = crypto::aes::ctr(KeySize::KeySize128, &key, &iv);
                let mut output = vec![];
                let read_buf = &mut RefReadBuffer::new(body_buf);
                let ref mut buffer = vec![0x00; 1024];

                let write_buf = &mut RefWriteBuffer::new(buffer);
                loop {
                    match ciphter.decrypt(read_buf, write_buf, true) {
                        Ok(result) => {
                            output.extend(
                                write_buf
                                    .take_read_buffer()
                                    .take_remaining()
                                    .iter()
                                    .map(|&i| i),
                            );
                            match result {
                                BufferResult::BufferUnderflow => break,
                                BufferResult::BufferOverflow => {}
                            }
                        }
                        Err(err) => return Err(CryptionError::SymmetricCipherError(err)),
                    }
                }

                Ok(bytes::Bytes::from(output))
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
                let mut ciphter = crypto::aes::ctr(KeySize::KeySize128, &key, &iv);
                let mut output = vec![];
                let read_buf = &mut RefReadBuffer::new(buf);
                let ref mut buffer = vec![0x00; 1024];

                let write_buf = &mut RefWriteBuffer::new(buffer);
                loop {
                    match ciphter.encrypt(read_buf, write_buf, true) {
                        Ok(result) => {
                            output.extend(
                                write_buf
                                    .take_read_buffer()
                                    .take_remaining()
                                    .iter()
                                    .map(|&i| i),
                            );
                            match result {
                                BufferResult::BufferUnderflow => break,
                                BufferResult::BufferOverflow => {}
                            }
                        }
                        Err(err) => return Err(CryptionError::SymmetricCipherError(err)),
                    }
                }

                Ok(bytes::Bytes::from(output))
            }
        }
    }
}

#[derive(Debug)]
pub enum CryptionError {
    SymmetricCipherError(SymmetricCipherError),
}

impl Display for CryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptionError::SymmetricCipherError(err) => err.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Cryption;

    #[test]
    fn test_cryption_decrypt() {
        let cryption = Cryption::Aes128Ctr {
            key: "a05a938346b456b8e2554efa33559157".to_string(),
            iv: "a05a938346b456b8e2554efa33559157".to_string(),
        };
        let r = cryption.encrypt("1234567".as_bytes());
        assert!(r.is_ok());

        let encrypt_data = r.expect("msg");

        let r = cryption.decrypt(&encrypt_data);
        assert!(r.is_ok());
        let decrypt_data = r.expect("msg");
        assert_eq!(&decrypt_data, "1234567".as_bytes());
    }
}
