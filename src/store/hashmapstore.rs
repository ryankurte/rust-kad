use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{self, Debug};

use crate::common::DatabaseId;
use crate::store::Datastore;

pub type Reducer<Data> = dyn Fn(&[Data]) -> Vec<Data> + Send + 'static;

pub struct HashMapStore<Id: Debug, Data: Debug> {
    data: HashMap<Id, Vec<Data>>,
    reducer: Option<Box<Reducer<Data>>>,
}

#[derive(Debug)]
pub struct DataEntry<Data, Meta> {
    value: Data,
    meta: Meta,
}

impl<Id, Data> HashMapStore<Id, Data>
where
    Id: DatabaseId + Debug,
    Data: PartialEq + Clone + Debug,
{
    /// Create a new HashMapStore without a reducer
    pub fn new() -> HashMapStore<Id, Data> {
        HashMapStore {
            data: HashMap::new(),
            reducer: None,
        }
    }

    /// Create a new HashMapStore with the provided reducer
    pub fn new_with_reducer(reducer: Box<Reducer<Data>>) -> HashMapStore<Id, Data> {
        HashMapStore {
            data: HashMap::new(),
            reducer: Some(reducer),
        }
    }

    /// Dump all data in the store
    pub fn dump(&self) -> Vec<(Id, Vec<Data>)> {
        self.data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl<Id, Data> Debug for HashMapStore<Id, Data>
where
    Id: DatabaseId + Debug,
    Data: PartialEq + Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl<Id, Data> Datastore<Id, Data> for HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: PartialEq + Clone + Debug,
{
    fn find(&self, id: &Id) -> Option<Vec<Data>> {
        let d = match self.data.get(id).map(|d| d.clone()) {
            Some(d) => d,
            None => return None,
        };
        if d.len() == 0 {
            return None;
        }
        Some(d)
    }

    fn store(&mut self, id: &Id, new: &Vec<Data>) -> Vec<Data> {
        let mut new = new.clone();

        match self.data.entry(id.clone()) {
            Entry::Vacant(v) => {
                v.insert(new.clone());

                new.clone()
            }
            Entry::Occupied(o) => {
                // Add new entry
                let existing = o.into_mut();
                existing.append(&mut new);

                // Reduce if provided
                if let Some(reducer) = &self.reducer {
                    let filtered = (reducer)(&existing);
                    *existing = filtered;
                }

                existing.clone()
            }
        }
    }
}
