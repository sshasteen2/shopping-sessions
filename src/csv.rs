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

pub fn records(data: &[u8], expected: usize) -> Vec<Vec<Vec<u8>>> {
    if data.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<&[u8]> = data.split(|b| *b == b'\n').collect();
    if let Some(last) = lines.last() {
        if last.is_empty() {
            lines.pop();
        }
    }
    if lines.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for line in lines.into_iter().skip(1) {
        if let Some(fields) = parse_record(line, expected) {
            out.push(fields.into_iter().map(|f| f.to_vec()).collect());
        }
    }
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
