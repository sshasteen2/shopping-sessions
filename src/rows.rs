//! Fast CSV rows under the spec's dialect.
//!
//! Every comma separates fields (no value may hold a comma, quote or newline),
//! so a row splits on commas alone. A field may then be wrapped in one pair of
//! quotes around the whole field; any other quote makes the row malformed.
//! Commas, quotes and newlines are found 64 bytes at a time with SIMD compares
//! and walked as bitmasks.

#[inline(always)]
fn masks(d: &[u8], at: usize) -> (u64, u64) {
    if at + 64 <= d.len() {
        // SAFETY: 64 bytes from `at` are in bounds.
        unsafe { masks64(d.as_ptr().add(at)) }
    } else {
        let mut buf = [0u8; 64];
        buf[..d.len() - at].copy_from_slice(&d[at..]);
        unsafe { masks64(buf.as_ptr()) }
    }
}

/// (comma-or-newline mask, quote mask) for 64 bytes at `p`.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    use std::arch::x86_64::*;
    let comma = _mm_set1_epi8(b',' as i8);
    let nl = _mm_set1_epi8(b'\n' as i8);
    let quote = _mm_set1_epi8(b'"' as i8);
    let mut dm = 0u64;
    let mut qm = 0u64;
    for i in 0..4 {
        let v = _mm_loadu_si128(p.add(16 * i) as *const __m128i);
        let d = _mm_or_si128(_mm_cmpeq_epi8(v, comma), _mm_cmpeq_epi8(v, nl));
        dm |= (_mm_movemask_epi8(d) as u32 as u64) << (16 * i);
        qm |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, quote)) as u32 as u64) << (16 * i);
    }
    (dm, qm)
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    let mut dm = 0u64;
    let mut qm = 0u64;
    for i in 0..64 {
        let b = *p.add(i);
        dm |= ((b == b',' || b == b'\n') as u64) << i;
        qm |= ((b == b'"') as u64) << i;
    }
    (dm, qm)
}

/// Calls `f` with the fields of every well-formed row of exactly `N` fields in
/// `d`. `header` skips the first line.
#[inline(always)]
#[allow(unused_assignments)]
pub fn for_each_row<'a, const N: usize>(d: &'a [u8], header: bool, mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut start = 0;
    if header {
        match memchr::memchr(b'\n', d) {
            Some(i) => start = i + 1,
            None => return,
        }
    }
    let len = d.len();
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut nf = 0usize; // fields finished in this line
    let mut fs = start; // current field start
    let mut q0 = usize::MAX; // first quote in this field
    let mut nq = 0u32; // quotes in this field
    let mut bad = false;
    let mut line_start = start;

    // Ends the field at `p` (a comma, a newline, or the end of data).
    macro_rules! end_field {
        ($p:expr, $eol:expr) => {{
            let p = $p;
            let field: &'a [u8] = if nq == 0 {
                &d[fs..p]
            } else if nq == 2 && q0 == fs && p >= fs + 2 && d[p - 1] == b'"' {
                &d[fs + 1..p - 1]
            } else {
                bad = true;
                &d[fs..fs]
            };
            if nf < N {
                fields[nf] = field;
            }
            nf += 1;
            if $eol {
                if !bad && nf == N && p > line_start {
                    f(&fields);
                }
                nf = 0;
                bad = false;
                line_start = p + 1;
            }
            fs = p + 1;
            nq = 0;
        }};
    }

    let mut blk = start;
    while blk < len {
        let (dm, qm) = masks(d, blk);
        let mut m = dm | qm;
        while m != 0 {
            let p = blk + m.trailing_zeros() as usize;
            m &= m - 1;
            match d[p] {
                b'"' => {
                    nq += 1;
                    if nq == 1 {
                        q0 = p;
                    }
                }
                b',' => end_field!(p, false),
                _ => end_field!(p, true),
            }
        }
        blk += 64;
    }
    if line_start < len {
        end_field!(len, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows<const N: usize>(s: &str) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        for_each_row::<N>(s.as_bytes(), false, |f| {
            out.push(f.iter().map(|x| String::from_utf8_lossy(x).into_owned()).collect())
        });
        out
    }

    #[test]
    fn dialect() {
        assert_eq!(rows::<2>("a,b\n"), vec![vec!["a", "b"]]);
        assert_eq!(rows::<2>("\"a\",\"\"\n"), vec![vec!["a", ""]]);
        assert_eq!(rows::<2>("a,b"), vec![vec!["a", "b"]]);
        assert_eq!(rows::<2>("a,\n"), vec![vec!["a", ""]]);
        assert!(rows::<2>("\n").is_empty());
        assert!(rows::<2>("a\n").is_empty());
        assert!(rows::<2>("a,b,c\n").is_empty());
        assert!(rows::<2>("a\"b,c\n").is_empty());
        assert!(rows::<2>("\"a,b\"\n").is_empty());
        assert!(rows::<2>("\"a\"x,b\n").is_empty());
        assert!(rows::<2>("\",b\n").is_empty());
        assert_eq!(rows::<2>(" a,b\r\n"), vec![vec![" a", "b\r"]]);
        // Rows across 64-byte blocks.
        let long = format!("{},{}\n{},y\n", "x".repeat(70), "z".repeat(60), "\"q\"");
        assert_eq!(rows::<2>(&long).len(), 2);
    }
}
