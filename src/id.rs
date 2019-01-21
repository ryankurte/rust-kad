/**
 * rust-kad
 * Database ID definitions
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::hash::Hash;
use std::fmt::Debug;
use std::ops::BitXor;

use num::Zero;
use num::bigint::{BigUint};

/// Id trait must be implemented for viable id types
pub trait DatabaseId: Hash + PartialEq + Eq + Ord + Clone + Send + Sync + Debug {
    /// Exclusive or two IDs to calculate distance
    fn xor(a: &Self, b: &Self) -> Self;
    /// Calculate number of bits to express a given ID
    fn bits(&self) -> usize;
    /// Check if a hash is zero
    fn is_zero(&self) -> bool;
}

/// DatabaseId implementation for u64
/// This is only for testing use
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
}

/// DatabaseId implementation for arbitrary Vectors
impl DatabaseId for Vec<u8> {
    fn xor(a: &Vec<u8>, b: &Vec<u8>) -> Vec<u8> {
        let a = BigUint::from_bytes_le(&a);
        let b = BigUint::from_bytes_le(&b);
        let c = a ^ b;
        c.to_bytes_le()
    }

    fn bits(&self) -> usize {
        let a = BigUint::from_bytes_le(self);
        a.bits()
    }

    fn is_zero(&self) -> bool {
        let a = BigUint::from_bytes_le(self);
        Zero::is_zero(&a)
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

            fn bits(&self) -> usize {
                let a = num::bigint::BigUint::from_bytes_le(self);
                a.bits()
            }

            fn is_zero(&self) -> bool {
                let a = BigUint::from_bytes_le(self);
                Zero::is_zero(&a)
            }
        }
    )
}

// Generate implementations for common sizes
database_id_slice!(32 / 8);
database_id_slice!(64 / 8);
database_id_slice!(128 / 8);
database_id_slice!(256 / 8);

use rand::distributions::{Standard, Distribution};

pub trait RequestId: Hash + PartialEq + Eq + Ord + Clone + Send + Sync + Debug {
    fn generate() -> Self;
}

impl RequestId for u64 {
    fn generate()-> Self {
        rand::random()
    }
}


impl RequestId for [u8; 8] {
    fn generate()-> Self {
        rand::random()
    }
}

