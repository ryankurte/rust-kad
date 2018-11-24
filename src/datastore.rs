/**
 * rust-kad
 * Generic datastore definitions and implementations
 *
 * https://github.com/ryankurte/rust-kad
 * Copyright 2018 Ryan Kurte
 */


use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;

pub trait Datastore<ID, DATA> {
    fn new() -> Self;
    fn find(&self, id: &ID) -> Option<Vec<DATA>>;
    fn store(&mut self, id: &ID, data: Vec<DATA>);
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

pub type HashMapStore<ID, DATA> = HashMap<ID, Vec<DATA>>;


impl <ID, DATA> Datastore<ID, DATA> for HashMapStore<ID, DATA>
where
    ID: Clone + Debug + std::cmp::Eq + std::hash::Hash,
    DATA: Updates + PartialEq + Clone + Debug,
{
    fn new() -> HashMap<ID, Vec<DATA>> {
        HashMap::new()
    }

    fn find(&self, id: &ID) -> Option<Vec<DATA>> {
        self.get(id).map(|d| d.clone() )
    }

    fn store(&mut self, id: &ID, new: Vec<DATA>) {
        match self.entry(id.clone()) {
            Entry::Vacant(v) => {
                v.insert(new.clone()); 
            },
            Entry::Occupied(o) => {
                let existing = o.into_mut();
                *existing = merge(existing, &new);
            }
        };
    }
}

fn merge<DATA>(original: &Vec<DATA>, new: &Vec<DATA>) -> Vec<DATA> 
where
    DATA: Updates + PartialEq + Clone,
{
    let mut merged: Vec<DATA> = vec![];

    // Update and add existing
    for o in original {
        let mut found = false;
        for n in new {
            if n.is_update(o) {
                merged.push(n.clone());
                found = true;
            }
        }
        if !found {
            merged.push(o.clone());
        }
    }

    // Add new unique entries
    for n in new {
        let mut found = false;
        for o in original {
            if n == o {
                found = true;
            }
        }
        if !found {
            merged.push(n.clone());
        }
    }

    merged
}