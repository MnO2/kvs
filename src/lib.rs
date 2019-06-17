use std::collections::hash_map::HashMap;

pub struct KvStore {
    map: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new()
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|x| x.to_owned())
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
