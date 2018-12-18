
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;

use crate::id::DatabaseId;
use crate::datastore::{Datastore, Reducer};

pub struct HashMapStore<ID, DATA> {
    data: HashMap<ID, Vec<DATA>>, 
}

impl <ID, DATA> HashMapStore<ID, DATA>
where
    ID: DatabaseId,
    DATA: Reducer + PartialEq + Clone + Debug,
{
    pub fn new() -> HashMapStore<ID, DATA> {
        HashMapStore{ data: HashMap::new() }
    }
}

impl <ID, DATA> Datastore<ID, DATA> for HashMapStore<ID, DATA>
where
    ID: DatabaseId,
    DATA: Reducer<Item=DATA> + PartialEq + Clone + Debug,
{
    
    fn find(&self, id: &ID) -> Option<Vec<DATA>> {
        self.data.get(id).map(|d| d.clone() )
    }

    fn store(&mut self, id: &ID, new: &Vec<DATA>) {
        let mut new = new.clone();
        
        match self.data.entry(id.clone()) {
            Entry::Vacant(v) => {
                v.insert(new.clone()); 
            },
            Entry::Occupied(o) => {
                let existing = o.into_mut();
                existing.append(&mut new);
                DATA::reduce(existing);
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
