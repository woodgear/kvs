use std::collections::HashMap;
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, val: String) {
        self.map.insert(key, val);
    }

    pub fn get(&self, key: String) -> Option<String> {
        self.map.get(&key).map(|v| v.clone())
    }

    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
