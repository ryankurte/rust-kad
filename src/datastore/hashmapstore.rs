
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;

use crate::id::DatabaseId;
use crate::datastore::{Datastore, Updates};

pub struct HashMapStore<ID, DATA> {
    data: HashMap<ID, Vec<DATA>>   
}

impl <ID, DATA> HashMapStore<ID, DATA>
where
    ID: DatabaseId,
    DATA: Updates + PartialEq + Clone + Debug,
{
    pub fn new() -> HashMapStore<ID, DATA> {
        HashMapStore{ data: HashMap::new() }
    }
}

impl <ID, DATA> Datastore<ID, DATA> for HashMapStore<ID, DATA>
where
    ID: DatabaseId,
    DATA: Updates + PartialEq + Clone + Debug,
{
    
    fn find(&self, id: &ID) -> Option<Vec<DATA>> {
        self.data.get(id).map(|d| d.clone() )
    }

    fn store(&mut self, id: &ID, new: &Vec<DATA>) {
        match self.data.entry(id.clone()) {
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
    DATA: Updates + PartialEq + Clone + Debug,
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