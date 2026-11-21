//! Spec CSV dialect: records end on `\n` only; `\r` is payload.

pub fn parse_record(line: &[u8], expected: usize) -> Option<Vec<&[u8]>> {
    if line.is_empty() {
        return None;
    }
    let mut fields = Vec::with_capacity(expected);
    let n = line.len();
    let mut i = 0;
    loop {
        if i < n && line[i] == b'"' {
            i += 1;
            let start = i;
            while i < n && line[i] != b'"' {
                i += 1;
            }
            if i >= n {
                return None;
            }
            fields.push(&line[start..i]);
            i += 1;
            if i == n {
                break;
            }
            if line[i] != b',' {
                return None;
            }
            i += 1;
            continue;
        }
        let start = i;
        while i < n && line[i] != b',' {
            if line[i] == b'"' {
                return None;
            }
            i += 1;
        }
        fields.push(&line[start..i]);
        if i == n {
            break;
        }
        i += 1;
        if i == n {
            fields.push(&line[n..n]);
            break;
        }
    }
    if fields.len() != expected {
        None
    } else {
        Some(fields)
    }
}

pub fn parse_into<'a>(line: &'a [u8], expected: usize, out: &mut [&'a [u8]]) -> bool {
    if line.is_empty() || expected == 0 || expected > out.len() {
        return false;
    }
    let n = line.len();
    let mut i = 0;
    let mut f = 0;
    loop {
        if f >= expected {
            return i >= n && f == expected;
        }
        if i < n && line[i] == b'"' {
            i += 1;
            let start = i;
            while i < n && line[i] != b'"' {
                i += 1;
            }
            if i >= n {
                return false;
            }
            out[f] = &line[start..i];
            f += 1;
            i += 1;
            if i == n {
                break;
            }
            if line[i] != b',' {
                return false;
            }
            i += 1;
            continue;
        }
        let start = i;
        while i < n && line[i] != b',' {
            if line[i] == b'"' {
                return false;
            }
            i += 1;
        }
        out[f] = &line[start..i];
        f += 1;
        if i == n {
            break;
        }
        i += 1;
        if i == n {
            if f >= expected {
                return false;
            }
            out[f] = &line[n..n];
            f += 1;
            break;
        }
    }
    f == expected
}

fn split_unquoted<'a>(line: &'a [u8], expected: usize, out: &mut [&'a [u8]]) -> bool {
    if expected == 0 || expected > out.len() {
        return false;
    }
    let mut f = 0;
    let mut start = 0;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if f >= expected {
                return false;
            }
            out[f] = &line[start..i];
            f += 1;
            start = i + 1;
        }
    }
    if f >= expected {
        return false;
    }
    out[f] = &line[start..];
    f += 1;
    f == expected
}

pub fn for_each_record(data: &[u8], expected: usize, mut f: impl FnMut(&[&[u8]])) {
    if data.is_empty() {
        return;
    }
    let mut start = 0;
    let mut header = true;
    while start <= data.len() {
        let rest = &data[start..];
        if rest.is_empty() {
            break;
        }
        let rel = memchr::memchr(b'\n', rest);
        let (line, next) = match rel {
            Some(i) => (&rest[..i], start + i + 1),
            None => (rest, data.len() + 1),
        };
        if header {
            header = false;
            start = next;
            if rel.is_none() {
                break;
            }
            continue;
        }
        let mut fields: [&[u8]; 8] = [&[]; 8];
        let ok = if memchr::memchr(b'"', line).is_none() {
            split_unquoted(line, expected, &mut fields)
        } else {
            parse_into(line, expected, &mut fields)
        };
        if ok {
            f(&fields[..expected]);
        }
        if rel.is_none() {
            break;
        }
        start = next;
    }
}

pub fn records(data: &[u8], expected: usize) -> Vec<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for_each_record(data, expected, |fields| {
        out.push(fields.iter().map(|f| f.to_vec()).collect());
    });
    out
}

pub fn field_str(f: &[u8]) -> Option<&str> {
    if f.is_empty() {
        None
    } else {
        std::str::from_utf8(f).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cr_is_payload() {
        let rows = records(b"h1,h2\nv,1000\r\n", 2);
        assert_eq!(rows, vec![vec![b"v".to_vec(), b"1000\r".to_vec()]]);
    }

    #[test]
    fn header_lookalike_is_data() {
        let rows = records(b"user_id_a,user_id_b\nuser_id_a,user_id_b\n", 2);
        assert_eq!(
            rows,
            vec![vec![b"user_id_a".to_vec(), b"user_id_b".to_vec()]]
        );
    }

    #[test]
    fn quoted_tab() {
        let got = parse_record(b"\"A\tB\",user_id_a", 2).unwrap();
        assert_eq!(got[0], b"A\tB");
        assert_eq!(got[1], b"user_id_a");
    }

    #[test]
    fn unbalanced_quote_dropped() {
        assert!(parse_record(b"u1,u5\"", 2).is_none());
    }

    #[test]
    fn wrong_field_count() {
        assert!(parse_record(b"u1,u2,u3", 2).is_none());
        assert!(parse_record(b"u1", 2).is_none());
    }

    #[test]
    fn spaces_kept() {
        let got = parse_record(b" u1,u1 ", 2).unwrap();
        assert_eq!(got[0], b" u1");
        assert_eq!(got[1], b"u1 ");
    }

    #[test]
    fn quotes_are_syntax() {
        let got = parse_record(b"\"u1\",\"u1\"", 2).unwrap();
        assert_eq!(got[0], b"u1");
        assert_eq!(got[1], b"u1");
    }

    #[test]
    fn empty_field() {
        let got = parse_record(b"u1,", 2).unwrap();
        assert_eq!(got[0], b"u1");
        assert!(got[1].is_empty());
    }

    #[test]
    fn empty_line() {
        assert!(parse_record(b"", 2).is_none());
    }
}
