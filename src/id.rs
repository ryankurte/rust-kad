

use core::hash::Hash;
use core::fmt::Debug;
use core::ops::BitXor;

use num::bigint::BigUint;

/// Id trait must be implemented for viable id types
pub trait DatabaseId: Hash + PartialEq + Eq + Ord + Clone + Send + Sync + Debug {
    /// Exclusive or two IDs to calculate distance
    fn xor(a: &Self, b: &Self) -> Self;
    /// Calculate number of bits to express a given ID
    fn bits(a: &Self) -> usize;
}

/// DatabaseId implementation for u64
/// This is only for testing use
impl DatabaseId for u64 {
    fn xor(a: &u64, b: &u64) -> u64 {
        a ^ b
    }

    fn bits(a: &Self) -> usize {
        (64 - a.leading_zeros()) as usize
    }
}

/// DatabaseId implementation for arbitrary Vectors
impl DatabaseId for Vec<u8> {
    fn xor(a: &Vec<u8>, b: &Vec<u8>) -> Vec<u8> {
        let a = BigUint::from_bytes_le(&a);
        let b = BigUint::from_bytes_le(&b);
        let c = a ^ b;
        c.to_bytes_le()
    }

    fn bits(a: &Self) -> usize {
        let a = BigUint::from_bytes_le(&a);
        a.bits()
    }
}

/// helper macro to generate DatabaseId impl over [u8; N] types
macro_rules! database_id_slice {
    ($N: expr) => (
        impl crate::id::DatabaseId for [u8; $N] {
            fn xor(a: &Self, b: &Self) -> Self {
                let mut c: Self = [0u8; $N];
                for i in 0..c.len() {
                    c[i] = a[i] ^ b[i];
                }
                c
            }

            fn bits(a: &Self) -> usize {
                let a = num::bigint::BigUint::from_bytes_le(a);
                a.bits()
            }
        }
    )
}

// Generate implementations for common sizes
database_id_slice!(32 / 8);
database_id_slice!(64 / 8);
database_id_slice!(128 / 8);
database_id_slice!(256 / 8);

pub trait RequestId: Hash + BitXor + PartialEq + Eq + Ord + Clone + Send + Sync + Debug {

}

