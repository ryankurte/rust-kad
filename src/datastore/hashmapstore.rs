
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;

use crate::id::DatabaseId;
use crate::datastore::{Datastore, Reducer};

pub struct HashMapStore<Id, Data> {
    data: HashMap<Id, Vec<Data>>, 
}

pub struct DataEntry<Data, Meta> {
    value: Data,
    meta: Meta,
}

impl <Id, Data> HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: Reducer + PartialEq + Clone + Debug,
{
    pub fn new() -> HashMapStore<Id, Data> {
        HashMapStore{ data: HashMap::new() }
    }
}

impl <Id, Data> Datastore<Id, Data> for HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: Reducer<Item=Data> + PartialEq + Clone + Debug,
{
    
    fn find(&self, id: &Id) -> Option<Vec<Data>> {
        self.data.get(id).map(|d| d.clone() )
    }

    fn store(&mut self, id: &Id, new: &Vec<Data>) {
        let mut new = new.clone();
        
        match self.data.entry(id.clone()) {
            Entry::Vacant(v) => {
                v.insert(new.clone()); 
            },
            Entry::Occupied(o) => {
                let existing = o.into_mut();
                existing.append(&mut new);
                Data::reduce(existing);
            }
        };
    }
}

impl Reducer for u64 {
    type Item = u64;

    fn reduce(v: &mut Vec<u64>) {
        v.sort();
        v.dedup();
    }
}
