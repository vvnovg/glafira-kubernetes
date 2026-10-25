//! Минимальный разбор DER: тег, содержимое и остаток (для срока сертификата Route).

pub fn read_tlv(input: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, rest) = input.split_first()?;
    let (&first, rest) = rest.split_first()?;
    let (len, rest) = if first < 0x80 {
        (first as usize, rest)
    } else {
        let n = (first & 0x7f) as usize;
        if n == 0 || n > 4 || rest.len() < n {
            return None;
        }
        let len = rest[..n].iter().fold(0usize, |a, b| (a << 8) | *b as usize);
        (len, &rest[n..])
    };
    if rest.len() < len {
        return None;
    }
    Some((tag, &rest[..len], &rest[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_and_long_lengths() {
        assert_eq!(
            read_tlv(&[0x04, 0x02, 1, 2, 9]),
            Some((0x04, &[1u8, 2][..], &[9u8][..]))
        );
        let mut long = vec![0x04, 0x81, 0x80];
        long.extend(std::iter::repeat_n(7u8, 0x80));
        assert_eq!(read_tlv(&long).unwrap().1.len(), 0x80);
        assert_eq!(read_tlv(&[0x04, 0x05, 1]), None);
    }
}
