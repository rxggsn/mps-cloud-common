use std::fmt::{Debug, Display, Write};

use aes::{
    Aes128,
    Aes256, cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher, StreamCipherError},
};
use aes_gcm_siv::aead;
use aes_gcm_siv::aead::{Aead, AeadMut};
use derive_new::new;

use crate::crypto::block_mode::gcm;
use crate::utils::codec::hex_string_as_slice;

pub mod block_mode {
    pub mod gcm {
        use aes::cipher::{
            BlockCipher, BlockEncrypt, generic_array::GenericArray, KeySizeUser, typenum::U16,
        };
        use aes::cipher::KeyInit;
        use aes_gcm_siv::{aead::Aead, AesGcmSiv, Nonce};

        use crate::crypto::CryptoError;

        macro_rules! gcm_siv {
            ($op:ident) => {
                pub fn $op<T>(
                    key: &[u8],
                    nonce: &[u8],
                    data: &[u8],
                ) -> Result<bytes::Bytes, CryptoError>
                where
                    T: BlockCipher<BlockSize = U16> + BlockEncrypt + KeyInit + KeySizeUser,
                {
                    let key = GenericArray::<u8, T::KeySize>::from_slice(key);
                    let nonce = Nonce::from_slice(nonce);
                    let cipher = AesGcmSiv::<T>::new(key);
                    cipher
                        .$op(nonce, data)
                        .map(|v| bytes::Bytes::from(v))
                        .map_err(|err| CryptoError::AEADCipherError(err))
                }
            };
        }

        gcm_siv!(encrypt);
        gcm_siv!(decrypt);
    }
}

#[derive(new)]
pub struct CryptoStates<'a> {
    pub nonce: Vec<u8>,
    pub crypto: &'a Crypto,
    pub data: &'a [u8],
}

impl<'a> CryptoStates<'a> {
    pub fn decrypt(&'a self) -> Result<bytes::Bytes, CryptoError> {
        self.crypto.decrypt(self.data, self.nonce.as_slice())
    }

    pub fn encrypt(&'a self) -> Result<bytes::Bytes, CryptoError> {
        self.crypto.encrypt(self.data, self.nonce.as_slice())
    }

    pub fn check_sign(&self, data: &[u8], signature: &[u8]) -> Result<bool, CryptoError> {
        self.crypto.check_sign(data, signature)
    }

    pub fn sign(&self, data: &[u8]) -> Result<bytes::Bytes, CryptoError> {
        self.crypto.sign(data)
    }
}

#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum Crypto {
    Unspecified,
    Aes128Ctr {
        key: String,
        iv: String,
    },
    AesGcmSiv {
        key: Vec<u8>,
    },
    Sm4GCM {
        key: Vec<u8>,
    },
    RSA {
        private_key: String,
        public_key: String,
    },
    Sm2 {
        key: Vec<u8>,
    },
}

impl Crypto {
    pub fn decrypt(&self, data: &[u8], nonce: &[u8]) -> Result<bytes::Bytes, CryptoError> {
        match self {
            Crypto::Aes128Ctr { key, iv } => {
                let key = hex_string_as_slice(key.as_str());
                let iv = hex_string_as_slice(iv.as_str());
                let mut cipher = ctr::Ctr128BE::<Aes128>::new(
                    &GenericArray::from_iter(key),
                    &GenericArray::from_iter(iv),
                );

                let mut output = vec![0u8; data.len()];
                cipher
                    .apply_keystream_b2b(data, &mut output)
                    .map(|_| bytes::Bytes::from_iter(output))
                    .map_err(|err| CryptoError::SymmetricCipherError(err))
            }
            Crypto::AesGcmSiv { key, .. } => {
                if key.len() == 16 {
                    gcm::decrypt::<Aes128>(key, nonce, data)
                } else if key.len() == 32 {
                    gcm::decrypt::<Aes256>(key, nonce, data)
                } else {
                    Err(CryptoError::InvalidLength(aes::cipher::InvalidLength))
                }
            }
            Crypto::Sm4GCM { key } => {
                let cipher = sm4::new(&key);
                let nonce = GenericArray::from_slice(nonce);
                cipher
                    .decrypt(nonce, data)
                    .map(|v| bytes::Bytes::from(v))
                    .map_err(|err| CryptoError::AEADCipherError(err))
            }
            _ => Err(CryptoError::CryptoNotSupportDecrypt),
        }
    }

    pub fn encrypt(&self, data: &[u8], nonce: &[u8]) -> Result<bytes::Bytes, CryptoError> {
        match self {
            Self::Unspecified => Ok(bytes::Bytes::copy_from_slice(data)),
            Self::Aes128Ctr { key, iv } => {
                let key = hex_string_as_slice(key.as_str());
                let iv = hex_string_as_slice(iv.as_str());
                let mut cipher = ctr::Ctr128BE::<Aes128>::new(
                    &GenericArray::from_iter(key),
                    &GenericArray::from_iter(iv),
                );

                let mut output = vec![0u8; data.len()];
                cipher
                    .apply_keystream_b2b(data, &mut output)
                    .map(|_| bytes::Bytes::from_iter(output))
                    .map_err(|err| CryptoError::SymmetricCipherError(err))
            }
            Self::AesGcmSiv { key, .. } => {
                if key.len() == 16 {
                    gcm::encrypt::<Aes128>(key, nonce, data)
                } else if key.len() == 32 {
                    gcm::encrypt::<Aes256>(key, nonce, data)
                } else {
                    Err(CryptoError::InvalidLength(aes::cipher::InvalidLength))
                }
            }
            Self::Sm4GCM { key } => {
                let cipher = sm4::new(&key);
                let nonce = GenericArray::from_slice(nonce);
                cipher
                    .encrypt(nonce, data)
                    .map(|v| bytes::Bytes::from(v))
                    .map_err(|err| CryptoError::AEADCipherError(err))
            }
            _ => Err(CryptoError::CryptoNotSupportEncrypt),
        }
    }

    pub fn check_sign(&self, data: &[u8], signature: &[u8]) -> Result<bool, CryptoError> {
        match self {
            Crypto::RSA { public_key, .. } => {
                use rsa::pkcs1::DecodeRsaPublicKey;
                use rsa::pkcs8::DecodePrivateKey;
                use rsa::{Pkcs1v15Sign, RsaPublicKey};

                let public_key = RsaPublicKey::from_pkcs1_pem(&public_key)
                    .map_err(|err| CryptoError::Pkcs8(err.to_string()))?;
                public_key
                    .verify(Pkcs1v15Sign::new(), data, signature)
                    .map_err(|err| CryptoError::RsaError(err))
                    .map(|_| true)
            }
            Self::Sm2 { key } => {
                use sm2::dsa::signature::Verifier;

                if signature.len() != sm2::dsa::Signature::BYTE_SIZE {
                    return Err(CryptoError::CorruptedSignature);
                }
                let mut sign = [0u8; sm2::dsa::Signature::BYTE_SIZE];
                sign.copy_from_slice(&signature);

                let mut pub_key = [0u8; 64];
                pub_key.copy_from_slice(&key[0..64]);

                sm2::dsa::VerifyingKey::from_sec1_bytes("", &pub_key)
                    .map_err(|err| CryptoError::Sm2Error(err.to_string()))?
                    .verify(
                        data,
                        &sm2::dsa::Signature::from_bytes(&sign)
                            .map_err(|err| CryptoError::Sm2Error(err.to_string()))?,
                    )
                    .map_err(|err| CryptoError::Sm2Error(err.to_string()))
                    .map(|_| true)
            }
            _ => Err(CryptoError::CryptoNotSupportSignature),
        }
    }

    pub fn sign(&self, data: &[u8]) -> Result<bytes::Bytes, CryptoError>{
        match self {
            Crypto::RSA { private_key, .. } => {
                use rsa::pkcs8::DecodePrivateKey;
                use rsa::{pkcs1v15::SigningKey, RsaPrivateKey, signature::{SignatureEncoding, Signer}};

                let private_key = RsaPrivateKey::from_pkcs8_pem(&private_key)
                    .map_err(|err| CryptoError::Pkcs8(err.to_string()))?;
                let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
                Ok(bytes::Bytes::from(signing_key.sign(data).to_vec()))
            }
            Self::Sm2 { key } => {
                use sm2::dsa::signature::Signer;
                use sm2::FieldBytes;

                let private_key = FieldBytes::from_slice(&key);

                sm2::dsa::SigningKey::from_bytes("", private_key)
                    .map_err(|err| CryptoError::Sm2Error(err.to_string()))?
                    .try_sign(data)
                    .map(|v| bytes::Bytes::from(v.to_bytes()))
                    .map_err(|err| CryptoError::Sm2Error(err.to_string()))
            }
            _ => Err(CryptoError::CryptoNotSupportSignature),
        }
    }
}

pub mod sm4 {
    use aes::cipher::Key;
    use aes_gcm_siv::AesGcmSiv;
    use sm4::cipher::KeyInit;
    use sm4::Sm4;

    pub fn new(key: &[u8]) -> AesGcmSiv<Sm4> {
        AesGcmSiv::new(Key::<Sm4>::from_slice(key))
    }
}

#[derive(Debug)]
pub enum CryptoError {
    SymmetricCipherError(StreamCipherError),
    InvalidLength(aes::cipher::InvalidLength),
    AEADCipherError(aead::Error),
    CryptoNotSupportEncrypt,
    CryptoNotSupportDecrypt,
    CryptoNotSupportSignature,
    RsaError(rsa::Error),
    Pkcs8(String),
    Sm2Error(String),
    CorruptedSignature,
}

impl Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::SymmetricCipherError(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::InvalidLength(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::AEADCipherError(err) =>f.write_fmt(format_args!("{}", err)),
            CryptoError::CryptoNotSupportEncrypt => f.write_str("encrypt not support"),
            CryptoError::CryptoNotSupportDecrypt => f.write_str("decrypt not support"),
            CryptoError::CryptoNotSupportSignature =>  f.write_str("signature not support"),
            CryptoError::RsaError(err) =>  f.write_fmt(format_args!("{}", err)),
            CryptoError::Pkcs8(err) => f.write_str(&err),
            CryptoError::Sm2Error(err) => f.write_str(&err),
            CryptoError::CorruptedSignature => f.write_str("corrupted signature"),
        }
    }
}

pub fn pkcs7_padding(data: &mut Vec<u8>, block_size: usize) {
    let padding = block_size - data.len() % block_size;
    data.resize(data.len() + padding, 0u8);
}

pub fn pkcs7_unpadding(data: &mut Vec<u8>) {
    let padding = data[data.len() - 1] as usize;
    data.truncate(data.len() - padding);
}

#[cfg(test)]
mod tests {
    use hex_literal::hex;

    use super::{Crypto, CryptoStates};

    #[test]
    fn test_aes128_ctr_crypto() {
        let crypto = Crypto::Aes128Ctr {
            key: "11111111111111111111111111111111".to_string(),
            iv: "0102030405060708090a0b0c0d0e0f10".to_string(),
        };
        let r = crypto.encrypt("123456789abcdefghijk".as_bytes(), vec![].as_slice());
        assert!(r.is_ok());

        let encrypt_data = r.expect("msg");

        let r = crypto.decrypt(&encrypt_data, vec![].as_slice());
        assert!(r.is_ok());
        let decrypt_data = r.expect("msg");
        assert_eq!(&decrypt_data, "123456789abcdefghijk".as_bytes());
    }

    #[test]
    fn test_aes128_gcm_siv_crypto() {
        let crypto = Crypto::AesGcmSiv {
            key: hex!("11111111111111111111111111111111").to_vec(),
        };

        let mut crypto_state = CryptoStates::new(
            "0123456789ab".as_bytes().to_vec(),
            &crypto,
            "123456789abcdefghijk".as_bytes(),
        );
        let r = crypto_state.encrypt();
        assert!(r.is_ok());

        let encrypt_data = r.expect("msg");
        crypto_state.data = &encrypt_data;

        let r = crypto_state.decrypt();
        assert!(r.is_ok());
        let decrypt_data = r.expect("msg");
        assert_eq!(&decrypt_data, "123456789abcdefghijk".as_bytes());
    }

    #[test]
    fn test_sm4_gcm_siv_crypto() {
        let crypto = Crypto::Sm4GCM {
            key: hex!("11111111111111111111111111111111").to_vec(),
        };

        let mut crypto_state = CryptoStates::new(
            "0123456789ab".as_bytes().to_vec(),
            &crypto,
            "123456789abcdefghijk".as_bytes(),
        );
        let r = crypto_state.encrypt();
        assert!(r.is_ok());

        let encrypt_data = r.expect("msg");
        crypto_state.data = &encrypt_data;

        let r = crypto_state.decrypt();
        assert!(r.is_ok());
        let decrypt_data = r.expect("msg");
        assert_eq!(&decrypt_data, "123456789abcdefghijk".as_bytes());
    }

    #[test]
    fn test_rsa_sign() {

    }
}
