use std::collections::HashMap;

use fnv::FnvHasher;
use std::hash::{Hash, Hasher};

pub struct Consistent {
    circle: HashMap<u64, String>,
    sorted_hash: Vec<u64>,
    replicas_num: u32,
}

impl Consistent {
    pub fn new() -> Consistent {
        Consistent {
            circle: HashMap::new(),
            replicas_num: 0,
            sorted_hash: Vec::new(),
        }
    }
    pub fn set_replicas_num(&mut self, num: u32) {
        if num <= 1 {
            self.replicas_num = 1;
        }
        self.replicas_num = num;
    }

    pub fn add(&mut self, name: String, weight: u32) {
        for i in 0..self.replicas_num {
            for j in 0..weight {
                let combined_string = format!("{}{}{}", i, j, name);
                let key = self.hash_key(&combined_string);
                self.circle.insert(key, name.clone());
            }
        }
        self.sort_hash_key()
    }

    pub fn get(&self, value: &String) -> Result<String, &'static str> {
        if self.circle.len() == 0 {
            return Err("empty circle");
        }
        let key = self.hash_key(value);
        let first_index = self.sorted_hash.binary_search_by(|x| {
            if *x >= key {
                std::cmp::Ordering::Greater
            } else if *x == key {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            }
        });

        match first_index {
            Ok(index) => {
                let key = self.sorted_hash.get(index).unwrap().clone();
                let value = self.circle.get(&key).unwrap().clone();
                Ok(value)
            }
            Err(index) => {
                if index < self.sorted_hash.len() {
                    let key = self.sorted_hash.get(index).unwrap().clone();
                    let value = self.circle.get(&key).unwrap().clone();
                    Ok(value)
                } else {
                    Err("not found")
                }
            }
        }
    }

    fn sort_hash_key(&mut self) {
        let mut v = self.sorted_hash.clone();
        let count = (self.replicas_num * 4) as usize;
        if self.sorted_hash.len() / count > self.circle.len() {
            v = Vec::new();
        }
        for (key, _) in self.circle.iter() {
            v.push(*key);
        }
        v.sort();
        self.sorted_hash = v
    }

    fn hash_key(&self, key: &String) -> u64 {
        let mut hasher = FnvHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod test {
    use std::fmt::format;

    use crate::Consistent;

    #[test]
    fn consistent_hash() {
        let mut instance = Consistent::new();
        instance.set_replicas_num(3);
        for i in 0..10 {
            instance.add(format!("{}{}", i, i), 10);
        }
        let r = instance.get(&String::from("kk")).unwrap();
        print!("{} ", r);
    }

    #[test]
    fn search() {
        let numbers = vec![1, 3, 5, 7, 9, 11];

        let search_value = 7;
        let first_index_greater_than_seven = numbers.binary_search_by(|x| {
            if *x > search_value {
                std::cmp::Ordering::Greater
            } else if *x == search_value {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            }
        });

        match first_index_greater_than_seven {
            Ok(index) => println!("First index greater than 7: {}", index),
            Err(index) => println!("No element greater than 7:{}", index),
        }
    }
}
