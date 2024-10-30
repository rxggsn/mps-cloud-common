use std::fmt::{Debug, Display, Write};

use aes::{
    Aes128,
    Aes256, cipher::{generic_array::GenericArray, KeyIvInit, StreamCipher, StreamCipherError},
};
use aes_gcm_siv::aead;
use aes_gcm_siv::aead::{Aead, AeadMut};
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use derive_new::new;
use rand::Rng;
use rsa::{RsaPrivateKey, RsaPublicKey};
use rsa::traits::PublicKeyParts;

use crate::ALPHABET;
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

    pub fn check_sign(&self, signature: &[u8]) -> Result<bool, CryptoError> {
        self.crypto.check_sign(self.data, signature)
    }

    pub fn sign(&self) -> Result<bytes::Bytes, CryptoError> {
        self.crypto.sign(self.data)
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
        private_key: Vec<u8>,
        public_key: Vec<u8>,
        account_id: String,
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
                use rsa::pkcs8::DecodePublicKey;
                use rsa::{
                    pkcs1v15::{Signature, VerifyingKey},
                    signature::Verifier,
                };

                let verify_key = VerifyingKey::<sha2::Sha256>::from_public_key_pem(&public_key)
                    .map_err(|err| CryptoError::LoadKeyError(err.to_string()))?;
                verify_key
                    .verify(
                        data,
                        &Signature::try_from(signature)
                            .map_err(|err| CryptoError::DSAError(err))?,
                    )
                    .map_err(|err| CryptoError::DSAError(err))
                    .map(|_| true)
            }
            Self::Sm2 {
                public_key,
                account_id,
                ..
            } => {
                use sm2::dsa::signature::Verifier;

                if signature.len() != sm2::dsa::Signature::BYTE_SIZE {
                    return Err(CryptoError::CorruptedSignature);
                }
                let mut sign = [0u8; sm2::dsa::Signature::BYTE_SIZE];
                sign.copy_from_slice(&signature);

                let mut pub_key = [0u8; 65];
                pub_key.copy_from_slice(&public_key[0..65]);

                sm2::dsa::VerifyingKey::from_sec1_bytes(&account_id, &pub_key)
                    .map_err(|err| CryptoError::LoadKeyError(err.to_string()))?
                    .verify(
                        data,
                        &sm2::dsa::Signature::from_bytes(&sign)
                            .map_err(|err| CryptoError::LoadSignatureError(err.to_string()))?,
                    )
                    .map_err(|err| CryptoError::Sm2Error(err.to_string()))
                    .map(|_| true)
            }
            _ => Err(CryptoError::CryptoNotSupportSignature),
        }
    }

    pub fn sign(&self, data: &[u8]) -> Result<bytes::Bytes, CryptoError> {
        match self {
            Crypto::RSA { private_key, .. } => {
                use rsa::pkcs8::DecodePrivateKey;
                use rsa::{
                    pkcs1v15::SigningKey,
                    RsaPrivateKey,
                    signature::{SignatureEncoding, Signer},
                };
                let private_key = RsaPrivateKey::from_pkcs8_pem(&private_key)
                    .map_err(|err| CryptoError::LoadKeyError(err.to_string()))?;
                let signing_key = SigningKey::<sha2::Sha256>::new(private_key);
                Ok(bytes::Bytes::from(signing_key.sign(data).to_vec()))
            }
            Self::Sm2 {
                private_key,
                account_id,
                ..
            } => {
                use sm2::dsa::signature::Signer;
                use sm2::FieldBytes;

                let private_key = FieldBytes::from_slice(&private_key);

                sm2::dsa::SigningKey::from_bytes(&account_id, private_key)
                    .map_err(|err| CryptoError::LoadSignatureError(err.to_string()))?
                    .try_sign(data)
                    .map(|v| bytes::Bytes::from(v.to_vec()))
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
    DSAError(rsa::signature::Error),
    Pkcs8(String),
    Sm2Error(String),
    CorruptedSignature,
    LoadKeyError(String),
    LoadSignatureError(String),
    RsaError(rsa::Error),
}

impl Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::SymmetricCipherError(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::InvalidLength(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::AEADCipherError(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::CryptoNotSupportEncrypt => f.write_str("encrypt not support"),
            CryptoError::CryptoNotSupportDecrypt => f.write_str("decrypt not support"),
            CryptoError::CryptoNotSupportSignature => f.write_str("signature not support"),
            CryptoError::DSAError(err) => f.write_fmt(format_args!("{}", err)),
            CryptoError::Pkcs8(err) => f.write_str(&err),
            CryptoError::Sm2Error(err) => f.write_str(&err),
            CryptoError::CorruptedSignature => f.write_str("corrupted signature"),
            CryptoError::LoadKeyError(err) => f.write_str(&err),
            CryptoError::LoadSignatureError(err) => f.write_str(&err),
            CryptoError::RsaError(err) => f.write_fmt(format_args!("{}", err)),
        }
    }
}

pub struct KeyPair {
    pub private_key: String,
    pub public_key: String,
}

pub fn pkcs7_padding(data: &mut Vec<u8>, block_size: usize) {
    let padding = block_size - data.len() % block_size;
    data.resize(data.len() + padding, 0u8);
}

pub fn pkcs7_unpadding(data: &mut Vec<u8>) {
    let padding = data[data.len() - 1] as usize;
    data.truncate(data.len() - padding);
}

pub fn base_64_encode(data: &[u8]) -> String {
    BASE64_STANDARD.encode(data)
}

pub fn base_64_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    BASE64_STANDARD.decode(data.as_bytes())
}

pub fn new_asymmetric_key_pair(bit_size: usize) -> Result<KeyPair, CryptoError> {
    use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey};
    let mut rng = rand::thread_rng();
    RsaPrivateKey::new(&mut rng, bit_size)
        .map_err(|err| CryptoError::RsaError(err))
        .and_then(|k| {
            let public_key = RsaPublicKey::from(&k);
            k.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                .map_err(|err| CryptoError::Pkcs8(err.to_string()))
                .and_then(|private_key| {
                    public_key
                        .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
                        .map(|public_key| KeyPair {
                            private_key: private_key.to_string(),
                            public_key,
                        })
                        .map_err(|err| CryptoError::Pkcs8(err.to_string()))
                })
        })
}

pub fn new_key(bit_size: usize) -> Vec<u8> {
    let mut key = vec![0u8; bit_size / 8];
    let characters = ALPHABET.chars().collect::<Vec<_>>();
    let length = ALPHABET.len();
    for i in 0..bit_size / 8 {
        let offset = rand::thread_rng().gen_range(0..length);
        key[i] = characters[offset] as u8;
    }

    key
}

pub fn new_iv() -> Vec<u8> {
    let mut key = vec![0u8; 16];
    let characters = ALPHABET.chars().collect::<Vec<_>>();
    let length = ALPHABET.len();
    for i in 0..16 {
        let offset = rand::thread_rng().gen_range(0..length);
        key[i] = characters[offset] as u8;
    }

    key
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::prelude::BASE64_STANDARD;
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
        let private_key = include_str!("../examples/rsa/private_key.pem").to_string();
        let public_key = include_str!("../examples/rsa/public_key.pem").to_string();
        let actual_signature = include_str!("../examples/rsa/signature").to_string();

        let crypto = Crypto::RSA {
            private_key,
            public_key,
        };

        let data = "123456789abcdefghijk".as_bytes();
        let sign = crypto.sign(data).expect("");
        let signature = BASE64_STANDARD.encode(sign);
        assert_eq!(signature, actual_signature)
    }

    #[test]
    fn test_sm2_sign() {
        let private_key = hex::decode(include_str!("../examples/sm2/private_key.hex")).expect("");
        let public_key = hex::decode(include_str!("../examples/sm2/public_key.hex")).expect("");

        let crypto = Crypto::Sm2 {
            private_key,
            public_key,
            account_id: "GGSN_RXLIGHT".to_string(),
        };
        let data = "123456789abcdefghijk".as_bytes();
        let sign = crypto.sign(data).expect("");

        assert!(crypto.check_sign(data, &sign).expect(""));
    }
    #[test]
    fn test_rsa_verify_sign() {
        let private_key = include_str!("../examples/rsa/private_key.pem").to_string();
        let public_key = include_str!("../examples/rsa/public_key.pem").to_string();
        let actual_signature = include_str!("../examples/rsa/signature").to_string();

        let crypto = Crypto::RSA {
            private_key,
            public_key,
        };
        let data = "123456789abcdefghijk".as_bytes();
        assert!(crypto
            .check_sign(
                data,
                &BASE64_STANDARD
                    .decode(actual_signature.as_bytes())
                    .expect("")
            )
            .expect(""));
    }

    #[test]
    fn test_sm2_verify_sign() {
        let private_key = hex::decode(include_str!("../examples/sm2/private_key.hex")).expect("");
        let public_key = hex::decode(include_str!("../examples/sm2/public_key.hex")).expect("");
        let actual_signature =
            hex::decode(include_str!("../examples/sm2/signature").replace(" ", "")).expect("");

        let crypto = Crypto::Sm2 {
            private_key,
            public_key,
            account_id: "GGSN_RXLIGHT".to_string(),
        };
        let data = b"123456789abcdefghijk";
        // println!("data: {}", hex::encode(data));
        // println!("distid: {}", hex::encode(DIST_ID));
        assert!(crypto.check_sign(data, &actual_signature).expect(""));
    }
}
