/**
 * rust-kad
 * Generic datastore definitions and implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */
use std::fmt::Debug;

use crate::common::DatabaseId;

pub mod hashmapstore;
pub use self::hashmapstore::HashMapStore;

/// Datastore trait for data storage implementations
pub trait Datastore<Id, Data>
where
    Id: DatabaseId,
    Data: PartialEq + Clone + Debug,
{
    // Find data by Id
    fn find(&self, id: &Id) -> Option<Vec<Data>>;

    // Store data by Id
    // This should add any new entries to the store and update
    // any matching / existing entries
    fn store(&mut self, id: &Id, data: &Vec<Data>) -> Vec<Data>;
}
