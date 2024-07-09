const XMODEM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);
const CRC32_ISO_HDLC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

pub fn crc_16_xmoden_checksum(buf: &[u8]) -> u16 {
    XMODEM.checksum(buf)
}

pub fn crc_32_iso_hdlc(buf: &[u8]) -> u32 {
    CRC32_ISO_HDLC.checksum(&buf)
}

#[cfg(test)]
mod tests {
    use crate::utils::codec::hex_to_u32;

    use super::crc_32_iso_hdlc;

    #[test]
    fn test_crc_32() {
        let r = crc_32_iso_hdlc(&[
            85, 170, 1, 0, 144, 0, 56, 0, 49, 1, 1, 0, 49, 2, 98, 2, 64, 96, 48, 1, 83, 77, 86, 50,
            71, 55, 51, 48, 66, 50, 48, 50, 49, 48, 56, 48, 48, 48, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 49, 1,
        ]);

        let actual = hex_to_u32(&[3, 158, 244, 109]);
        assert_eq!(r, actual);
    }
}
