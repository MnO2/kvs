pub struct KvStore {
    a: usize
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            a: 1
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        None
    }

    pub fn set(&mut self, key: String, value: String) {
        unimplemented!();
    }

    pub fn remove(&mut self, key: String) {
        unimplemented!();
    }
}
