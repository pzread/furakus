pub fn hex(bytes: &[u8]) -> String {
    const HEXLIST: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut hex_string = String::new();
    for byte in bytes {
        hex_string.push(HEXLIST[(byte >> 4) as usize]);
        hex_string.push(HEXLIST[(byte & 0xF) as usize]);
    }
    hex_string
}

pub fn unhex(hex_string: &str) -> Result<Vec<u8>, ()> {
    fn hex_to_u8(hex: u8) -> Result<u8, ()> {
        match hex {
            b'0'..=b'9' => Ok(hex - b'0'),
            b'a'..=b'f' => Ok(hex - b'a' + 10),
            _ => Err(()),
        }
    }
    if hex_string.len() % 2 != 0 {
        return Err(());
    }
    let mut bytes = Vec::new();
    for chr in hex_string.as_bytes().chunks(2) {
        let hi = hex_to_u8(chr[0])?;
        let lo = hex_to_u8(chr[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}
