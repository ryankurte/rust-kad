/**
 * rust-kad
 * Generic datastore definitions and implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */



pub mod hashmapstore;
pub use self::hashmapstore::HashMapStore;

/// Datastore trait for data storage implementations
pub trait Datastore<ID, DATA> {
    // Find data by ID
    fn find(&self, id: &ID) -> Option<Vec<DATA>>;

    // Store data by ID
    // This should add any new entries to the store and update
    // any existing entries if is_update is true
    fn store(&mut self, id: &ID, data: &Vec<DATA>);
}


pub trait Updates {
    fn is_update(&self, other: &Self) -> bool;
}

#[cfg(test)]
impl Updates for u64 {
    fn is_update(&self, other: &Self) -> bool {
        self > other
    }
}