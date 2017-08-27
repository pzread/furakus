use futures::Future;

pub trait BoxedFuture: Future + Send + Sized + 'static {
    fn boxed2(self) -> Box<Future<Item = Self::Item, Error = Self::Error> + Send> {
        Box::new(self)
    }
}

impl<T: Future + Send + 'static> BoxedFuture for T {}

pub fn hex(bytes: &[u8]) -> String {
    const HEXLIST: [char; 16] = [
        '0',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
    ];
    let mut hexstr = String::new();
    for byte in bytes {
        hexstr.push(HEXLIST[(byte >> 4) as usize]);
        hexstr.push(HEXLIST[(byte & 0xF) as usize]);
    }
    hexstr
}

pub fn unhex(hexstr: &str) -> Result<Vec<u8>, ()> {
    fn hex_to_u8(hex: u8) -> Result<u8, ()> {
        match hex {
            b'0'...b'9' => Ok(hex - b'0'),
            b'a'...b'f' => Ok(hex - b'a' + 10),
            _ => Err(()),
        }
    }
    if hexstr.len() % 2 != 0 {
        return Err(());
    }
    let mut bytes = Vec::new();
    for chr in hexstr.as_bytes().chunks(2) {
        let hi = hex_to_u8(chr[0])?;
        let lo = hex_to_u8(chr[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hexify() {
        let bytes: Vec<u8> = (0..256).map(|num: u32| num as u8).collect();
        const HEXLIST: [char; 16] = [
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
            '9',
            'a',
            'b',
            'c',
            'd',
            'e',
            'f',
        ];
        let mut hexstr = String::new();
        for hi in HEXLIST.iter() {
            for lo in HEXLIST.iter() {
                hexstr.push_str(&format!("{}{}", hi, lo));
            }
        }
        assert_eq!(hex(&bytes), hexstr);
        assert_eq!(unhex(&hexstr).unwrap(), bytes);

        assert_eq!(unhex(&"a"), Err(()));
        assert_eq!(unhex(&"za"), Err(()));
        assert_eq!(unhex(&"az"), Err(()));
        assert_eq!(unhex(&"??"), Err(()));
    }
}
