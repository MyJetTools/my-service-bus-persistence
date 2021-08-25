use std::collections::HashMap;

use lazy_static::lazy_static;

pub fn parse_query_string(result: &mut HashMap<String, String>, query_string: &str) {
    let elements = query_string.split("&");

    for el in elements {
        let kv = el.find('=');

        if let Some(index) = kv {
            let key = decode_from_url_string(&el[..index]);
            let value = decode_from_url_string(&el[index + 1..]);
            result.insert(key, value);
        }
    }
}

pub fn decode_from_url_string(src: &str) -> String {
    let index = src.find("%");

    if index.is_none() {
        return src.to_string();
    }

    let mut result: Vec<u8> = Vec::new();

    let mut escape_mode = false;

    let src = src.as_bytes();
    let mut pos = 0;
    while pos < src.len() {
        let c = src[pos];

        if escape_mode {
            pos += 1;
            if pos >= src.len() {
                return String::from_utf8(result).unwrap();
            }

            let c2 = src[pos];

            let c = decode_url_escape(c, c2);
            result.push(c);
            escape_mode = false;
        } else {
            if c == b'+' {
                result.push(' ' as u8);
            } else if c == b'%' {
                escape_mode = true;
            } else {
                result.push(c as u8);
            }
        }

        pos += 1;
    }

    return String::from_utf8(result).unwrap();
}

pub fn decode_url_escape(s0: u8, s1: u8) -> u8 {
    if s0 == b'2' {
        return URL_DECODE_SYMBOLS_2.get(&s1).unwrap().clone();
    }

    if s0 == b'3' {
        return URL_DECODE_SYMBOLS_3.get(&s1).unwrap().clone();
    }

    if s0 == b'4' && s1 == b'0' {
        return b'@';
    }

    if s0 == b'5' {
        if s1 == b'B' || s1 == b'b' {
            return b'[';
        }
        if s1 == b'D' || s1 == b'D' {
            return b']';
        }
    }

    panic!("Invalid URL Symbol %{}{}", s0 as char, s1 as char);
}

/*
pub fn encode_to_url_string_and_copy(res: &mut Vec<u8>, src: &str) {
    let mut has_symbol_to_encode = false;
    for (_, c) in src.chars().enumerate() {
        if URL_ENCODE_SYMBOLS.contains_key(&c) {
            has_symbol_to_encode = true;
            break;
        }
    }

    if !has_symbol_to_encode {
        res.extend(src.as_bytes());
        return;
    }

    for (_, c) in src.chars().enumerate() {
        let found = URL_ENCODE_SYMBOLS.get(&c);

        match found {
            Some(str) => {
                res.extend(str.as_bytes());
            }
            None => {
                res.push(c as u8);
            }
        }
    }
}
 */

lazy_static! {
    static ref URL_ENCODE_SYMBOLS: HashMap<char, &'static str> = [
        (' ', "+"),
        ('#', "%23"),
        ('$', "%24"),
        ('%', "%25"),
        ('&', "%26"),
        ('\'', "%27"),
        ('(', "%28"),
        (')', "%29"),
        ('*', "%2A"),
        ('+', "%2B"),
        (',', "%2C"),
        ('/', "%2F"),
        (':', "%3A"),
        (';', "%3B"),
        ('=', "%3D"),
        ('?', "%3F"),
        ('@', "%40"),
        ('[', "%5B"),
        (']', "%5D"),
    ]
    .iter()
    .copied()
    .collect();
}

lazy_static! {
    static ref URL_DECODE_SYMBOLS_2: HashMap<u8, u8> = [
        (b'3', b'#'),
        (b'4', b'$'),
        (b'5', b'%'),
        (b'6', b'&'),
        (b'7', b'\''),
        (b'8', b'('),
        (b'9', b')'),
        (b'A', b'*'),
        (b'a', b'*'),
        (b'B', b'+'),
        (b'b', b'+'),
        (b'C', b','),
        (b'c', b','),
        (b'F', b'/'),
        (b'f', b'/'),
    ]
    .iter()
    .copied()
    .collect();
}

lazy_static! {
    static ref URL_DECODE_SYMBOLS_3: HashMap<u8, u8> = [
        (b'A', b':'),
        (b'a', b':'),
        (b'B', b';'),
        (b'b', b';'),
        (b'D', b'='),
        (b'd', b'='),
        (b'F', b'?'),
        (b'f', b'?'),
//        ('@', "%40"),
//        ('[', "%5B"),
//        (']', "%5D"),
    ]
    .iter()
    .copied()
    .collect();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn test_parse_query_string() {
        let query_string = "topicId=testtopic&fromDate=2021-05-29T12%3A00%3A00&maxAmount=10";

        let mut result = HashMap::new();

        super::parse_query_string(&mut result, query_string);

        assert_eq!(3, result.len());
        assert_eq!("testtopic", result.get("topicId").unwrap());
        assert_eq!("2021-05-29T12:00:00", result.get("fromDate").unwrap());
        assert_eq!("10", result.get("maxAmount").unwrap());
    }
}
