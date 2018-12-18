/**
 * rust-kad
 * Generic datastore definitions and implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::fmt::Debug;

use crate::id::DatabaseId;

pub mod hashmapstore;
pub use self::hashmapstore::HashMapStore;

/// Datastore trait for data storage implementations
pub trait Datastore<ID, DATA> 
where
    ID: DatabaseId,
    DATA: Reducer<Item=DATA> + PartialEq + Clone + Debug,
{
    // Find data by ID
    fn find(&self, id: &ID) -> Option<Vec<DATA>>;

    // Store data by ID
    // This should add any new entries to the store and update
    // any matching / existing entries
    fn store(&mut self, id: &ID, data: &Vec<DATA>);
}

/// Reducer trait for performing MapReduce on data in the store
pub trait Reducer {
    type Item;
    fn reduce(&mut Vec<Self::Item>);
}