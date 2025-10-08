use std::ops::ControlFlow;

pub mod reader;
pub mod writer;

pub struct DataPack {
    pub(crate) _start: usize,
    pub(crate) _end: usize,
    pub data: bytes::Bytes,
}

impl DataPack {
    pub fn new(data: bytes::Bytes) -> Self {
        Self {
            _start: 0,
            _end: 0,
            data,
        }
    }
}

fn merge_package(read_buf: &mut bytes::BytesMut, data_buf: &mut bytes::BytesMut) {
    let need_capacity = read_buf.len() + data_buf.len();

    if read_buf.capacity() < need_capacity {
        read_buf.reserve(need_capacity - read_buf.capacity())
    }

    read_buf.extend_from_slice(&data_buf);
    data_buf.clear();
}

fn next_package(buf: &[u8], start_byte: u8, end_byte: u8) -> DataPack {
    let mut is_start = false;
    let mut is_end = false;
    let mut end_idx = 0;
    let mut start_idx = 0;
    let mut idx = 0;
    buf.iter().try_for_each(|b| {
        if start_byte == *b && !is_start {
            start_idx = idx;
            idx += 1;
            is_start = true;
            ControlFlow::Continue(())
        } else if end_byte == *b && is_start && !is_end {
            end_idx = idx + 1;
            is_end = true;
            ControlFlow::Break(())
        } else if is_start && !is_end {
            idx += 1;
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    });

    let data = &buf[start_idx..end_idx];

    DataPack {
        _start: start_idx,
        _end: end_idx,
        data: bytes::Bytes::copy_from_slice(data),
    }
}

fn truncate_package(buf: &mut bytes::BytesMut, package: &DataPack) {
    let len = buf.len();
    buf.reverse();
    buf.truncate(len - package._end);
    buf.reverse();
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::iox::{merge_package, next_package, truncate_package};

    #[test]
    fn test_merge_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        let read_buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75]);
        read_buf.put_slice(&[0x7c, 0x89, 0x72, 0x75]);
        merge_package(buf, read_buf);

        let buf: &[u8] = &buf;

        assert_eq!(&[0x7c, 0x89, 0x72, 0x75, 0x7c, 0x89, 0x72, 0x75], buf);
        assert!(read_buf.is_empty())
    }

    #[test]
    fn test_truncate_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75, 0x79, 0xff, 0x3e]);
        let package = super::DataPack {
            _start: 0,
            _end: 4,
            data: bytes::Bytes::new(),
        };
        truncate_package(buf, &package);
        assert_eq!(buf.to_vec(), vec![0x79, 0xff, 0x3e]);
    }

    #[test]
    fn test_next_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75, 0x79, 0xff, 0x3e, 0x7c, 0x98, 0x97]);
        let package = next_package(buf, 0x7c, 0x7c);
        assert_eq!(package._start, 0);
        assert_eq!(package._end, 8);
    }
}
