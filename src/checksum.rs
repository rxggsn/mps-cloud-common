const XMODEM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);

pub fn crc_16_xmoden_checksum(buf: &[u8]) -> u16 {
    XMODEM.checksum(buf)
}
