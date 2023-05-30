use std::fmt::Debug;
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

    /// Count number of non-zero bits required to express a given ID
    fn bits(&self) -> usize;

    /// Check if a hash is zero
    fn is_zero(&self) -> bool;

    /// Bit length of the ID type
    fn max_bits(&self) -> usize;

    /// Invert the bits in an ID
    fn inv(&self) -> Self;

    /// Mask the top N bits of an ID, used when calculating bucket addresses
    fn mask(&self, bits: usize) -> Self;

    /// Create a zero ID
    fn zero() -> Self;

    /// Create a max (all bits set) ID
    fn max() -> Self;

    fn from_bit(index: usize) -> Self;
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
        a.bits() as usize
    }

    fn is_zero(&self) -> bool {
        let a = BigUint::from_bytes_le(self.as_ref());
        Zero::is_zero(&a)
    }

    fn max_bits(&self) -> usize {
        self.as_ref().len() * 8
    }

    fn zero() -> Self {
        Self::default()
    }

    fn max() -> Self {
        let mut a = Self::default();

        for b in a.as_mut() {
            *b = 0xFF;
        }

        a
    }

    fn inv(&self) -> Self {
        let mut a = self.clone();

        for b in a.as_mut() {
            *b = !*b;
        }

        a
    }

    fn mask(&self, bits: usize) -> Self {
        let a = self.as_ref();
        let mut b = T::default();
        let c = b.as_mut();

        // Invert bits for mask computation
        // (ie. bits = 2 means retain two highest bits)
        let n = self.max_bits() - bits;

        for i in 0..a.len() {
            // Compute mask for a specific byte
            let n1 = n.saturating_sub(i * 8);
            let mask = if n1 >= 8 {
                0x00
            } else if n1 == 0 {
                0xFF
            } else {
                !((1 << (n1 % 8)) - 1)
            };

            c[i] = a[i] & mask;
        }

        b
    }

    fn from_bit(index: usize) -> Self {
        let mut a = Self::default();

        a.as_mut()[index / 8] = 1 << (index % 8);

        a
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_id_mask_u8() {
        let tests = &[
            ([0b1111_1111], 1, [0b1000_0000]),
            ([0b1111_1111], 2, [0b1100_0000]),
            ([0b1111_1111], 3, [0b1110_0000]),
            ([0b1111_1111], 7, [0b1111_1110]),
            ([0b1111_1111], 8, [0b1111_1111]),
        ];

        for (a, m, b) in tests {
            let v = a.mask(*m);
            assert_eq!(&v, b, "actual: {:08b} expected: {:08b}", v[0], b[0]);
        }
    }

    #[test]
    fn test_id_mask_u16() {
        let tests = &[
            (1, [0b0000_0000, 0b1000_0000]),
            (2, [0b0000_0000, 0b1100_0000]),
            (3, [0b0000_0000, 0b1110_0000]),
            (7, [0b0000_0000, 0b1111_1110]),
            (8, [0b0000_0000, 0b1111_1111]),
            (9, [0b1000_0000, 0b1111_1111]),
            (10, [0b1100_0000, 0b1111_1111]),
            (14, [0b1111_1100, 0b1111_1111]),
            (15, [0b1111_1110, 0b1111_1111]),
            (16, [0b1111_1111, 0b1111_1111]),
        ];

        for (mask, expected) in tests {
            let a = [0b1111_1111, 0b1111_1111];

            let v = a.mask(*mask);
            assert_eq!(&v, expected, "actual: {:x?} expected: {:x?}", v, expected);
        }
    }

    #[test]
    fn test_id_mask_u32() {
        let tests = &[
            (3, [0b0000_0000, 0b0000_0000, 0b0000_0000, 0b1110_0000]),
            (11, [0b0000_0000, 0b0000_0000, 0b1110_0000, 0b1111_1111]),
            (19, [0b0000_0000, 0b1110_0000, 0b1111_1111, 0b1111_1111]),
            (27, [0b1110_0000, 0b1111_1111, 0b1111_1111, 0b1111_1111]),
        ];

        for (mask, expected) in tests {
            let a = [0b1111_1111, 0b1111_1111, 0b1111_1111, 0b1111_1111];

            let v = a.mask(*mask);
            assert_eq!(&v, expected, "actual: {:x?} expected: {:x?}", v, expected);
        }
    }
}
