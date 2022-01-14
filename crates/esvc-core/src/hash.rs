use serde::{Deserialize, Serialize};
use std::fmt;

/// When dealing with new events which use a different hash than others,
/// keep in mind that the hash will thus differ, and they won't be merged
/// inside of the graph. This can be mitigated by migrating all graph nodes
/// or by strictly reusing graph nodes, however, the performance penality
/// might massively exceed the compatiblity benefit.
#[repr(C)]
#[serde_with::serde_as]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Hash {
    Blake2b512(#[serde_as(as = "serde_with::Bytes")] [u8; 64]),
}

const HASH_B64_CFG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);
const HASH_BLK2512_PFX: &str = "blake2b512:";

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, bytes) = match self {
            Hash::Blake2b512(ref x) => (HASH_BLK2512_PFX, x),
        };
        write!(f, "{}{}", kind, base64::encode_config(bytes, HASH_B64_CFG))
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq)]
pub enum HashDecodeError {
    #[error("base64 decoding error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("concrete hash part is too short (got {got}, expected {expected})")]
    TooShort { got: usize, expected: usize },

    #[error("invalid hash prefix '{0}'")]
    InvalidPrefix(String),
}

impl core::str::FromStr for Hash {
    type Err = HashDecodeError;

    fn from_str(s: &str) -> Result<Hash, HashDecodeError> {
        if let Some(x) = s.strip_prefix(HASH_BLK2512_PFX) {
            let mut buf = [0u8; 64];
            let dcl = base64::decode_config_slice(x, HASH_B64_CFG, &mut buf).map_err(|x| {
                use base64::DecodeError as Bdce;
                let offset = HASH_BLK2512_PFX.len();
                match x {
                    Bdce::InvalidByte(a, b) => Bdce::InvalidByte(offset + a, b),
                    Bdce::InvalidLength => Bdce::InvalidLength,
                    Bdce::InvalidLastSymbol(a, b) => Bdce::InvalidLastSymbol(offset + a, b),
                }
            })?;
            if dcl < buf.len() {
                return Err(HashDecodeError::TooShort {
                    got: x.len(),
                    expected: buf.len(),
                });
            }
            Ok(Hash::Blake2b512(buf))
        } else {
            let truncp = s.find(':').unwrap_or(s.len());
            Err(HashDecodeError::InvalidPrefix(s[..truncp].to_string()))
        }
    }
}

// TODO: make it possible to select which hash should be used
pub fn calculate_hash(dat: &[u8]) -> Hash {
    use blake2::Digest;
    let mut hasher = blake2::Blake2b512::new();
    hasher.update(dat);
    let tmp = hasher.finalize();
    let mut ret = [0u8; 64];
    ret.copy_from_slice(tmp.as_slice());
    Hash::Blake2b512(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_parse_err_invalid_prefix() {
        assert_eq!(
            "hello:1234".parse::<Hash>(),
            Err(HashDecodeError::InvalidPrefix("hello".to_string()))
        );
    }

    #[test]
    fn hash_parse_err_base64() {
        assert_eq!(
            "blake2b512:.".parse::<Hash>(),
            Err(HashDecodeError::Base64(base64::DecodeError::InvalidByte(
                11, b'.'
            )))
        );
    }

    const GTH: Hash = Hash::Blake2b512([
        207, 114, 247, 238, 107, 232, 17, 55, 229, 186, 214, 166, 184, 208, 96, 252, 67, 32, 28,
        203, 113, 194, 111, 24, 149, 157, 137, 127, 183, 118, 121, 156, 14, 32, 34, 132, 138, 243,
        141, 153, 87, 76, 109, 145, 247, 109, 108, 230, 13, 210, 5, 38, 56, 76, 18, 41, 96, 233,
        122, 235, 55, 66, 107, 150,
    ]);

    #[test]
    fn ex0_calc_hash() {
        assert_eq!(calculate_hash("Guten Tag!".as_bytes()), GTH);
    }

    const GTH_STR: &str = "blake2b512:z3L37mvoETflutamuNBg_EMgHMtxwm8YlZ2Jf7d2eZwOICKEivONmVdMbZH3bWzmDdIFJjhMEilg6XrrN0Jrlg";

    #[test]
    fn ex0_hash_str() {
        assert_eq!(GTH.to_string(), GTH_STR);
        assert_eq!(GTH_STR.parse::<Hash>(), Ok(GTH));
    }
}
