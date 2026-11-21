use ahash::AHashMap;

#[derive(Default)]
pub struct Intern {
    map: AHashMap<Vec<u8>, u32>,
    pub bytes: Vec<Vec<u8>>,
}

impl Intern {
    pub fn get(&mut self, s: &[u8]) -> u32 {
        if let Some(&i) = self.map.get(s) {
            return i;
        }
        let i = self.bytes.len() as u32;
        let owned = s.to_vec();
        self.map.insert(owned.clone(), i);
        self.bytes.push(owned);
        i
    }

    pub fn get_str(&self, i: u32) -> String {
        String::from_utf8_lossy(&self.bytes[i as usize]).into_owned()
    }
}
