
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::{Arc, Mutex};


use crate::common::DatabaseId;
use crate::store::Datastore;

pub type Reducer<Data> = dyn Fn(&[Data]) -> Vec<Data> + Send + 'static;

#[derive(Clone)]
pub struct HashMapStore<Id: fmt::Debug, Data: fmt::Debug> {
    data: Arc<Mutex<HashMap<Id, Vec<Data>>>>,
    reducer: Option<Arc<Mutex<Box<Reducer<Data>>>>>,
}

#[derive(Debug)]
pub struct DataEntry<Data, Meta> {
    value: Data,
    meta: Meta,
}

impl <Id, Data> HashMapStore<Id, Data>
where
    Id: DatabaseId + fmt::Debug,
    Data: PartialEq + Clone + fmt::Debug,
{
    /// Create a new HashMapStore without a reducer
    pub fn new() -> HashMapStore<Id, Data> {
        HashMapStore{ data: Arc::new(Mutex::new(HashMap::new())), reducer: None }
    }

    /// Create a new HashMapStore with the provided reducer
    pub fn new_with_reducer(reducer: Box<Reducer<Data>>) -> HashMapStore<Id, Data> {
        HashMapStore{ data: Arc::new(Mutex::new(HashMap::new())), reducer: Some(Arc::new(Mutex::new(reducer))) }
    }

    /// Dump all data in the store
    pub fn dump(&self) -> Vec<(Id, Vec<Data>)> {
        let data = self.data.lock().unwrap();
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

impl <Id, Data> fmt::Debug for HashMapStore<Id, Data> 
where 
    Id: DatabaseId + fmt::Debug,
    Data: PartialEq + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let data = self.data.lock().unwrap();
        write!(f, "{:?}", data)
    }
}

impl <Id, Data> Datastore<Id, Data> for HashMapStore<Id, Data>
where
    Id: DatabaseId,
    Data: PartialEq + Clone + fmt::Debug,
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
                let existing = o.into_mut();
                existing.append(&mut new);

                // Reduce if provided
                if let Some(reducer) = self.reducer.clone() {
                    let reducer = reducer.lock().unwrap();
                    let filtered = (reducer)(&existing);
                    *existing = filtered;
                }

                existing.clone()
            }
        }
    }
}
