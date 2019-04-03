
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};


use crate::common::DatabaseId;
use crate::store::Datastore;

pub type Reducer<Data> = Fn(&mut Vec<Data>) + Send + 'static;

#[derive(Clone)]
pub struct HashMapStore<Id, Data> {
    data: Arc<Mutex<HashMap<Id, Vec<Data>>>>,
    reducer: Option<Arc<Mutex<Box<Reducer<Data>>>>>,
}

pub struct DataEntry<Data, Meta> {
    value: Data,
    meta: Meta,
}

impl <Id, Data> HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: PartialEq + Clone + Debug,
{
    /// Create a new HashMapStore without a reducer
    pub fn new() -> HashMapStore<Id, Data> {
        HashMapStore{ data: Arc::new(Mutex::new(HashMap::new())), reducer: None }
    }

    /// Create a new HashMapStore with the provided reducer
    pub fn new_with_reducer(reducer: Box<Reducer<Data>>) -> HashMapStore<Id, Data> {
        HashMapStore{ data: Arc::new(Mutex::new(HashMap::new())), reducer: Some(Arc::new(Mutex::new(reducer))) }
    }
}

impl <Id, Data> Datastore<Id, Data> for HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: PartialEq + Clone + Debug,
{
    
    fn find(&self, id: &Id) -> Option<Vec<Data>> {
        let data = self.data.lock().unwrap();
        let d = match data.get(id).map(|d| d.clone() ) {
            Some(d) => d,
            None => return None,
        };
        if d.len() == 0 {
            return None
        }
        Some(d)
    }

    fn store(&mut self, id: &Id, new: &Vec<Data>) -> Vec<Data> {
        let mut new = new.clone();
        let mut data = self.data.lock().unwrap();
        
        match data.entry(id.clone()) {
            Entry::Vacant(v) => {
                v.insert(new.clone());

                new.clone()
            },
            Entry::Occupied(o) => {
                // Add new entry
                let mut existing = o.into_mut();
                existing.append(&mut new);

                // Reduce if provided
                if let Some(reducer) = self.reducer.clone() {
                    let reducer = reducer.lock().unwrap();
                    (reducer)(&mut existing);
                }

                existing.clone()
            }
        }
    }
}
