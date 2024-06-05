const XMODEM: crc::Crc<u16> = crc::Crc::<u16>::new(&crc::CRC_16_XMODEM);
const CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_CKSUM);

pub fn crc_16_xmoden_checksum(buf: &[u8]) -> u16 {
    XMODEM.checksum(buf)
}

pub fn crc_32(buf: &[u8]) -> u32 {
    CRC32.checksum(&buf)
}
