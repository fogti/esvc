use rkyv::{ser::serializers::AllocSerializer, util::AlignedVec, Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

pub use rkyv;

// NOTE: avoid changing these data types, it would influence the data format
// which means that hashes or some other relevant stuff would change.
// We don't want that. (especially this one)
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct Event {
    pub name: u128,
    pub arg: Vec<u8>,
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub struct EventWithDeps {
    pub ev: Event,
    pub deps: BTreeSet<u128>,
}

/// When dealing with new events which use a different hash than others,
/// keep in mind that the hash will thus differ, and they won't be merged
/// inside of the graph. This can be mitigated by migrating all graph nodes
/// or by strictly reusing graph nodes, however, the performance penality
/// might massively exceed the compatiblity benefit.
#[repr(C)]
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq)]
pub enum Hash {
    Blake2b512([u8; 64]),
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, bytes) = match self {
            Hash::Blake2b512(ref x) => ("blake2b512", x),
        };
        write!(f, "{}:{}", kind, hex::encode(&bytes[..]))
    }
}

pub fn to_bytes<T, const N: usize>(x: &T) -> Result<AlignedVec, String>
where
    T: rkyv::Serialize<AllocSerializer<N>>,
{
    rkyv::to_bytes::<T, N>(x).map_err(|e| e.to_string())
}

// TODO: make it possible to select which hash should be used
pub fn calculate_hash(av: &AlignedVec) -> Hash {
    use blake2::Digest;
    let mut hasher = blake2::Blake2b512::new();
    hasher.update(av.as_slice());
    let tmp = hasher.finalize();
    let mut ret = [0u8; 64];
    ret.copy_from_slice(tmp.as_slice());
    Hash::Blake2b512(ret)
}

pub fn uuid_from_hash(av: &AlignedVec) -> uuid::Uuid {
    use blake2::Digest;
    let mut hasher = blake2::Blake2b::<blake2::digest::consts::U16>::new();
    hasher.update(av.as_slice());
    let tmp = hasher.finalize();
    let mut ret = [0u8; 16];
    ret.copy_from_slice(tmp.as_slice());
    uuid::Uuid::from_bytes(ret)
}

pub mod state;
