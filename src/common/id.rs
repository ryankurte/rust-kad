use std::fmt::{Debug};
/**
 * rust-kad
 * Database ID definitions
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::hash::Hash;

use num::bigint::BigUint;
use num::Zero;

/// Id trait must be implemented for viable id types
pub trait DatabaseId: Hash + Default + PartialEq + Eq + Ord + Clone + Send + Debug {
    /// Exclusive or two IDs to calculate distance
    fn xor(a: &Self, b: &Self) -> Self;
    /// Calculate number of bits to express a given ID
    fn bits(&self) -> usize;
    /// Check if a hash is zero
    fn is_zero(&self) -> bool;

    /// Bit length of the ID type
    fn max_bits(&self) -> usize;
}

/// DatabaseId implementation for u64
/// This is only for testing use
#[cfg(nope)]
impl DatabaseId for u64 {
    fn xor(a: &u64, b: &u64) -> u64 {
        a ^ b
    }

    fn bits(&self) -> usize {
        (64 - self.leading_zeros()) as usize
    }

    fn is_zero(&self) -> bool {
        Zero::is_zero(self)
    }

    fn max_bits(&self) -> usize {
        64
    }
}

/// DatabaseId implementation for arbitrary types around &[u8]
impl<T> DatabaseId for T
where
    T: AsRef<[u8]>
        + AsMut<[u8]>
        + Hash
        + Default
        + PartialEq
        + Eq
        + Ord
        + Clone
        + Sync
        + Send
        + Debug,
{
    fn xor(a: &T, b: &T) -> Self {
        let a = a.as_ref();
        let b = b.as_ref();
        let mut c = T::default();

        {
            let c = c.as_mut();
            assert!(
                a.len() == b.len() && a.len() == c.len(),
                "dht IDs must be the same length"
            );

            for i in 0..a.len() {
                c[i] = a[i] ^ b[i];
            }
        }

        c
    }

    fn bits(&self) -> usize {
        let a = BigUint::from_bytes_le(self.as_ref());
        a.bits()
    }

    fn is_zero(&self) -> bool {
        let a = BigUint::from_bytes_le(self.as_ref());
        Zero::is_zero(&a)
    }

    fn max_bits(&self) -> usize {
        self.as_ref().len() * 8
    }
}

pub trait RequestId: Hash + PartialEq + Eq + Ord + Clone + Send + Debug {
    fn generate() -> Self;
}

impl RequestId for u8 {
    fn generate() -> Self {
        rand::random()
    }
}

impl RequestId for u16 {
    fn generate() -> Self {
        rand::random()
    }
}

impl RequestId for u32 {
    fn generate() -> Self {
        rand::random()
    }
}

impl RequestId for u64 {
    fn generate() -> Self {
        rand::random()
    }
}

impl RequestId for [u8; 8] {
    fn generate() -> Self {
        rand::random()
    }
}
