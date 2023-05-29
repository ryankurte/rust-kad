/**
 * rust-kad
 * Kademlia Node Table Implementation
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::fmt::Debug;
use std::ops::Range;
use std::time::Instant;

use crate::common::{DatabaseId, Entry};

use super::kbucket::KBucket;
use super::nodetable::{BucketInfo, NodeTable};

/// KNodeTable Implementation
/// This uses a flattened approximation whereby buckets are pre-allocated and indexed by their distance from the local Id
/// as a simplification of the allocation based branching approach introduced in the paper.
//
// TODO: some assumptions here are not ideal, for example, bucket 0 will only ever contain the single entry where target == id.
// and the lower index buckets may not be possible to fill to bucket_size / should probably offset bucket count based on bucket
// size to compensate (eg. if buckets are sized for two entries, reduce distance by 1 bit)
pub struct KNodeTable<Id, Info> {
    id: Id,
    buckets: Vec<KBucket<Id, Info>>,
    hash_size: usize,
    bucket_size: usize,
}

impl<Id, Info> KNodeTable<Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    /// Create a new KNodeTable with the provided bucket and hash sizes
    // TODO: hash_size approx isn't -quite- right and the use of the wrong hash size results in bucket indexing errors...
    // perhaps this could be abstracted / const generic-ified to make it more resilient?
    pub fn new(id: Id, bucket_size: usize, hash_size: usize) -> KNodeTable<Id, Info> {
        // Create a new bucket and assign own Id
        let buckets = vec![KBucket::new(bucket_size)];
        // Generate KNodeTable object
        KNodeTable {
            id,
            buckets,
            bucket_size,
            hash_size,
        }
    }

    // Calculate the distance between two Ids.
    fn distance(a: &Id, b: &Id) -> Id {
        Id::xor(a, b)
    }

    /// Fetch a reference to the bucket containing the provided Id
    fn bucket(&self, id: &Id) -> &KBucket<Id, Info> {
        let index = self.bucket_index(id);
        &self.buckets[index]
    }

    /// Fetch a mutable reference to the bucket containing the provided Id
    fn bucket_mut(&mut self, id: &Id) -> &mut KBucket<Id, Info> {
        let index = self.bucket_index(id);
        &mut self.buckets[index]
    }

    /// Fetch the bucket index for a given Id
    /// This is basically just the number of bits in the distance between the local and target Ids
    fn bucket_index(&self, id: &Id) -> usize {
        // Find difference
        let diff = KNodeTable::<Id, Info>::distance(&self.id, id);

        // Count bits of difference for distance
        let dist = diff.bits();

        // Limit by number of buckets
        let count = self.buckets.len();

        // Compute bucket index
        let n = dist_to_idx(self.hash_size, count, dist);

        println!("Bucket id {id:?} diff: {diff:?} dist: {dist} index: {n} (/{count})");

        n
    }

    #[allow(dead_code)]
    fn update_buckets(&self) {
        unimplemented!()
    }

    /// Split the deepest bucket (always last in the list)
    fn split(&mut self) {
        println!("Split bucket {}", self.buckets.len() - 1);

        // Remove old deepest bucket
        let old_bucket = self.buckets.pop().unwrap();

        // Create new split buckets
        self.buckets.push(KBucket::new(self.bucket_size));
        self.buckets.push(KBucket::new(self.bucket_size));

        // Re-insert nodes
        for n in old_bucket.nodes() {
            let bucket = self.bucket_mut(n.id());
            bucket.create_or_update(&n);
        }
    }

    /// Check whether a split is required
    fn check_split(&self, id: &Id) -> bool {
        // Lookup bucket containing node
        let bucket_index = self.bucket_index(id);

        // Only the deepest bucket can be split
        if bucket_index != self.buckets.len() - 1 || self.buckets.len() >= self.hash_size {
            return false;
        }

        // Only required if the bucket does not already contain the node and is full
        if self.buckets[bucket_index].find(id).is_some()
            || self.buckets[bucket_index].node_count() < self.bucket_size
        {
            return false;
        }

        return true;
    }

    /// Compute the base [Id] for a bucket by index
    fn bucket_id(&self, index: usize) -> Id {
        let count = self.buckets.len();

        // Last bucket always centred around us
        if index == count - 1 {
            return self.id.clone();
        }

        // Prior buckets differ by first n bits

        // Compute bit to flip
        let flip = Id::from_bit(self.hash_size - index - 1);

        // Compute flipped ID
        let inv = Id::xor(&self.id, &flip);

        // Mask out lower bits
        let n = index + 1;
        let m = inv.mask(n);

        println!("index: {index:} inv: {inv:?} n: {n:} m: {m:?}");

        m
    }
}

impl<Id, Info> NodeTable<Id, Info> for KNodeTable<Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    type NodeIter<'a> = KNodeIter<'a, Id, Info>;

    /// Fetch the number of buckets in the NodeTable
    fn buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Create or update a node in the NodeTable
    fn create_or_update(&mut self, node: &Entry<Id, Info>) -> bool {
        if node.id() == &self.id {
            return false;
        }
        let node_id = node.id();

        // Check whether the target bucket needs to be split
        if self.check_split(node_id) {
            self.split();
        }

        println!("Create/update node {node:?}");

        // Create / update the node
        let mut node = node.clone();
        node.set_seen(Instant::now());

        let bucket = self.bucket_mut(&node_id);
        let updated = bucket.create_or_update(&node);

        if !updated {
            println!("No space in bucket for node {:?}", node_id);
        }

        updated
    }

    /// Find the nearest nodes to the provided Id in the given range
    fn nearest(&self, id: &Id, range: Range<usize>) -> Vec<Entry<Id, Info>> {
        // TODO: this is exceedingly inefficient, replace with iter and filters.

        // Create a list of all nodes
        let mut all: Vec<_> = self
            .buckets
            .iter()
            .flat_map(|b| b.nodes())
            //.filter(|n| n.id() != &self.id )
            .collect();
        let count = all.len();

        // Sort by distance
        all.sort_by_key(|n| KNodeTable::<Id, Info>::distance(id, n.id()));

        // Limit to count or total found
        let mut range = range;
        range.end = usize::min(count, range.end);

        let limited = all.drain(range).collect();
        limited
    }

    /// Check if the node NodeTable contains a given node by Id
    /// This returns the node object if found
    fn contains(&self, id: &Id) -> Option<Entry<Id, Info>> {
        let bucket = self.bucket(id);
        bucket.find(id)
    }

    /// Fetch the oldest node in the specified bucket
    fn oldest(&self, index: usize) -> Option<Entry<Id, Info>> {
        self.buckets[index].oldest()
    }

    /// Update an entry by ID
    fn update_entry<F>(&mut self, id: &Id, f: F) -> bool
    where
        F: Fn(&mut Entry<Id, Info>),
    {
        let bucket = self.bucket_mut(id);
        bucket.update_entry(id, f)
    }

    /// Remove an entry by ID
    fn remove_entry(&mut self, id: &Id) {
        let bucket = self.bucket_mut(id);
        bucket.remove_entry(id, false);
    }

    /// Fetch information from each bucket
    fn bucket_info(&self, index: usize) -> Option<BucketInfo<Id>> {
        if index >= self.buckets.len() {
            return None;
        }

        let b = &self.buckets[index];

        Some(BucketInfo {
            index,
            id: self.bucket_id(index),
            nodes: b.node_count(),
            updated: b.updated(),
        })
    }

    /// Iterate through entries in the node table
    fn entries<'b>(&'b self) -> Self::NodeIter<'b> {
        KNodeIter {
            ctx: self,
            bucket_index: 0,
            entry_index: 0,
        }
    }
}

pub struct KNodeIter<'a, Id, Info> {
    ctx: &'a KNodeTable<Id, Info>,
    bucket_index: usize,
    entry_index: usize,
}

impl<'a, Id, Info> Iterator for KNodeIter<'a, Id, Info>
where
    Id: DatabaseId + Clone + 'static,
    Info: Clone + Debug + 'static,
{
    type Item = &'a Entry<Id, Info>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have no buckets, return
        if self.ctx.buckets.len() == 0 {
            return None;
        }

        let mut current = &self.ctx.buckets[self.bucket_index];

        // If we're out of entries in a bucket, look for the next bucket
        if self.entry_index >= current.node_count() {
            // Find the next bucket containing one or more nodes
            let mut next_bucket = 0;
            for i in self.bucket_index + 1..self.ctx.buckets.len() {
                if self.ctx.buckets[i].node_count() > 0 {
                    next_bucket = i;
                    break;
                }
            }

            // If we have no more nodes, return none
            if next_bucket == 0 {
                return None;
            }

            // Update the bucket index
            current = &self.ctx.buckets[next_bucket];
            self.bucket_index = next_bucket;
            self.entry_index = 0;
        }

        // Grab the current entry
        let v = current.node(self.entry_index);

        // Increment entry index
        self.entry_index += 1;

        Some(v)
    }
}

/// Compute bucket index from distance metric
fn dist_to_idx(hash_bits: usize, bucket_count: usize, dist: usize) -> usize {
    let offset = hash_bits - dist;
    let n = offset.min(bucket_count - 1);
    n
}

#[cfg(test)]
mod test {
    use super::*;
    use super::{KNodeTable, NodeTable};

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
    struct TestId([u8; 1]);

    impl Debug for TestId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "0b{:08b}", self.0[0])
        }
    }

    impl From<u8> for TestId {
        fn from(value: u8) -> Self {
            Self([value])
        }
    }

    impl AsRef<[u8]> for TestId {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl AsMut<[u8]> for TestId {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
    }

    #[test]
    fn test_id() {
        let mut a = TestId::from(0b1000_0001);
        a.as_mut()[0] = 0;
        assert_eq!(a.as_ref(), &[0]);
    }

    #[test]
    fn test_distances() {
        let tests = &[
            ([0b1000], [0b1001], [1]),
            ([0b1000], [0b1010], [2]),
            ([0b1000], [0b1011], [3]),
            ([0b1000], [0b1100], [4]),
            ([0b1000], [0b0000], [8]),
            ([0b1000], [0b0001], [9]),
            ([0b1000], [0b0010], [10]),
            ([0b1000], [0b0011], [11]),
            ([0b1000], [0b0100], [12]),
            ([0b1000], [0b0101], [13]),
            ([0b1000], [0b0110], [14]),
            ([0b1000], [0b0111], [15]),
        ];

        for (a, b, d) in tests {
            let d1 = KNodeTable::<[u8; 1], u64>::distance(&a, &b);
            assert_eq!(&d1, d);
        }
    }

    #[test]
    fn test_dist_to_idx() {
        let tests = &[
            // Single bucket, everything ends up there
            (4, 1, 4, 0),
            (4, 1, 3, 0),
            (4, 1, 2, 0),
            (4, 1, 1, 0),
            (4, 1, 0, 0),
            // Two buckets, dist >= 4 in first, everything else
            // in the second
            (4, 2, 4, 0),
            (4, 2, 3, 1),
            (4, 2, 2, 1),
            (4, 2, 1, 1),
            (4, 2, 0, 1),
            // Three buckets, dist >= 4 in the first, dist >= 3
            // in the second, everything else in the third
            (4, 3, 4, 0),
            (4, 3, 3, 1),
            (4, 3, 2, 2),
            (4, 3, 1, 2),
            (4, 3, 0, 2),
        ];

        for (bits, count, dist, idx) in tests {
            let n = dist_to_idx(*bits, *count, *dist);

            assert_eq!(n, *idx);
        }
    }

    #[test]
    fn test_bucket_indicies_simple() {
        let mut table = KNodeTable::<TestId, u64>::new(0b1000_0000.into(), 2, 8);

        // With a single bucket, everything ends up in the same place
        assert_eq!(table.bucket_index(&TestId::from(0b1001_0000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b0001_0000)), 0);

        println!("2x");

        // With two buckets, nearer values end up in the second
        table.buckets.push(KBucket::new(table.bucket_size));

        assert_eq!(table.bucket_index(&TestId::from(0b1000_0001)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b1010_0000)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b0101_0000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b0111_0000)), 0);

        println!("3x");

        // With three buckets, nearest values end up in the third
        table.buckets.push(KBucket::new(table.bucket_size));

        assert_eq!(table.bucket_index(&TestId::from(0b1000_0001)), 2);
        assert_eq!(table.bucket_index(&TestId::from(0b1100_0001)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b1110_0000)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b1111_0000)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b0001_0000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b0011_0000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b0111_0000)), 0);
    }

    #[test]
    fn test_bucket_indicies_simple_inv() {
        let mut table = KNodeTable::<TestId, u64>::new(0b0000.into(), 2, 4);

        // With a single bucket, everything ends up in the same place
        assert_eq!(table.bucket_index(&TestId::from(0b1001)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b0001)), 0);

        println!("2x");

        // With two buckets, nearer values end up in the second
        table.buckets.push(KBucket::new(table.bucket_size));

        assert_eq!(table.bucket_index(&TestId::from(0b0001)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b0010)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b0100)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b1000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b1001)), 0);

        println!("3x");

        // With three buckets, nearest values end up in the third
        table.buckets.push(KBucket::new(table.bucket_size));

        assert_eq!(table.bucket_index(&TestId::from(0b0001)), 2);
        assert_eq!(table.bucket_index(&TestId::from(0b0010)), 2);
        assert_eq!(table.bucket_index(&TestId::from(0b0100)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b0111)), 1);
        assert_eq!(table.bucket_index(&TestId::from(0b1000)), 0);
        assert_eq!(table.bucket_index(&TestId::from(0b1111)), 0);
    }

    #[test]
    fn test_k_node_buckets() {
        let mut table = KNodeTable::<[u8; 1], u64>::new([0b1000], 2, 4);

        let tests = &[
            // First pair end up in the same bucket
            ([0b0001], 0),
            ([0b1001], 0),
            // Next node causes a split
            ([0b0010], 0),
            // Then two bit distance
            ([0b1010], 1),
            // Causing another split
            ([0b1101], 1),
            ([0b1011], 2),
        ];

        for (id, index) in tests {
            println!("Insert {:?} expected bucket {}", id, index);

            table.create_or_update(&Entry::new(*id, 0u64));

            assert_eq!(
                table.bucket_index(&id),
                *index,
                "Expected bucket {index} for id {id:?}"
            );
        }
    }

    #[test]
    fn test_k_bucket_id() {
        let mut t = KNodeTable::<TestId, u64>::new(TestId::from(0b0100_0001), 2, 8);

        // One bucket, located at our ID
        assert_eq!(t.bucket_id(0), TestId::from(0b0100_0001));

        println!("2x");

        t.buckets.push(KBucket::new(t.bucket_size));

        assert_eq!(t.bucket_id(0), TestId::from(0b1000_0000));
        assert_eq!(t.bucket_id(1), TestId::from(0b0100_0001));

        println!("3x");

        t.buckets.push(KBucket::new(t.bucket_size));

        let t3 = &[(0, 0b1000_0000), (1, 0b0000_0000), (2, 0b0100_0001)];

        for (b, i) in t3 {
            let expected_id = TestId::from(*i);

            let bucket_id = t.bucket_id(*b);
            assert_eq!(bucket_id, expected_id);

            assert_eq!(t.bucket_index(&bucket_id), *b);
        }

        println!("4x");

        t.buckets.push(KBucket::new(t.bucket_size));

        let t4 = &[
            (0, 0b1000_0000),
            (1, 0b0000_0000),
            (2, 0b0110_0000),
            (3, 0b0100_0001),
        ];

        for (b, i) in t4 {
            let expected_id = TestId::from(*i);

            let bucket_id = t.bucket_id(*b);
            assert_eq!(bucket_id, expected_id);

            assert_eq!(t.bucket_index(&bucket_id), *b);
        }
    }

    #[test]
    fn test_k_node_table() {
        let n = Entry::new([0b0100], 1);

        let mut t = KNodeTable::<[u8; 1], u64>::new(*n.id(), 10, 4);

        let nodes = vec![
            Entry::new([0b0000], 1),
            Entry::new([0b0001], 2),
            Entry::new([0b0110], 3),
            Entry::new([0b1011], 4),
        ];

        // Add some nodes
        for n in &nodes {
            assert!(t.contains(n.id()).is_none());
            assert!(t.create_or_update(n));
            assert_eq!(*n, t.contains(n.id()).unwrap());
        }

        // Find closest nodes
        assert_eq!(
            vec![nodes[2].clone(), nodes[0].clone()],
            t.nearest(n.id(), 0..2)
        );
        assert_eq!(
            vec![nodes[0].clone(), nodes[1].clone()],
            t.nearest(&[0b0010], 0..2)
        );
    }

    #[test]
    fn test_k_node_iter() {
        let n = Entry::new([0b1000], 1);

        let mut t = KNodeTable::<[u8; 1], u64>::new(*n.id(), 2, 4);

        let nodes = vec![
            Entry::new([0b1001], 1),
            Entry::new([0b0001], 2),
            Entry::new([0b0100], 3),
            Entry::new([0b1010], 4),
            Entry::new([0b1100], 5),
            Entry::new([0b1101], 6),
        ];

        // Add some nodes
        for (i, n) in nodes.iter().enumerate() {
            t.create_or_update(n);

            let mut n1: Vec<_> = t.entries().map(|e| e.clone()).collect();
            n1.sort_by_key(|e| e.id().clone());

            let mut n2 = nodes[..i + 1].to_vec();
            n2.sort_by_key(|e| e.id().clone());

            assert_eq!(&n1, &n2);
        }
    }
}
