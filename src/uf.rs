use ahash::AHashMap;

#[derive(Default)]
pub struct People {
    intern: AHashMap<Vec<u8>, u32>,
    bytes: Vec<Vec<u8>>,
    parent: Vec<u32>,
    rank: Vec<u8>,
    min_id: Vec<u32>,
}

impl People {
    pub fn intern(&mut self, id: &[u8]) -> u32 {
        if let Some(&i) = self.intern.get(id) {
            return i;
        }
        let i = self.bytes.len() as u32;
        self.intern.insert(id.to_vec(), i);
        self.bytes.push(id.to_vec());
        self.parent.push(i);
        self.rank.push(0);
        self.min_id.push(i);
        i
    }

    fn find(&mut self, mut x: u32) -> u32 {
        let mut root = x;
        while self.parent[root as usize] != root {
            root = self.parent[root as usize];
        }
        while x != root {
            let p = self.parent[x as usize];
            self.parent[x as usize] = root;
            x = p;
        }
        root
    }

    pub fn union(&mut self, a: u32, b: u32) {
        let mut ra = self.find(a);
        let mut rb = self.find(b);
        if ra == rb {
            return;
        }
        if self.rank[ra as usize] < self.rank[rb as usize] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb as usize] = ra;
        if self.rank[ra as usize] == self.rank[rb as usize] {
            self.rank[ra as usize] += 1;
        }
        let ma = self.min_id[ra as usize];
        let mb = self.min_id[rb as usize];
        self.min_id[ra as usize] = if self.bytes[ma as usize] <= self.bytes[mb as usize] {
            ma
        } else {
            mb
        };
    }

    pub fn name_of(&mut self, id: u32) -> &[u8] {
        let r = self.find(id);
        let m = self.min_id[r as usize];
        &self.bytes[m as usize]
    }

    pub fn name_str(&mut self, id: u32) -> String {
        String::from_utf8_lossy(self.name_of(id)).into_owned()
    }

    pub fn root(&mut self, id: u32) -> u32 {
        let r = self.find(id);
        self.min_id[r as usize]
    }
}
