use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use crate::app::AppError;

pub fn decompress_payload(payload: &[u8]) -> Result<Vec<u8>, AppError> {
    let c = Cursor::new(payload.to_vec());

    let zip = zip::ZipArchive::new(c);

    if let Err(err) = zip {
        return Err(AppError::Other {
            msg: format!("Can not open zip file. Err: {:?}", err),
        });
    }

    let mut zip = zip.unwrap();

    let mut page_buffer: Vec<u8> = Vec::new();

    for i in 0..zip.len() {
        let zip_file = zip.by_index(i);

        if let Err(err) = zip_file {
            return Err(AppError::Other {
                msg: format!("Can not unzip payload. Err: {:?}", err),
            });
        }

        let mut zip_file = zip_file.unwrap();

        if zip_file.name() == "d" {
            let mut buffer = [0u8; 1024 * 1024];

            loop {
                let read_size = zip_file.read(&mut buffer[..]).unwrap();
                if read_size == 0 {
                    break;
                }

                page_buffer.extend(&buffer[..read_size]);
            }
        }
    }

    Ok(page_buffer)
}

pub fn compress_payload(payload: &[u8]) -> Result<Vec<u8>, AppError> {
    let mut writer = VecWriter::new();

    {
        let mut zip = zip::ZipWriter::new(&mut writer);

        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        zip.start_file("d", options).unwrap();

        let mut pos = 0;
        while pos < payload.len() {
            let size = zip.write(&payload[pos..]).unwrap();

            pos += size;
        }

        zip.finish().unwrap();
    }

    Ok(writer.buf)
}

pub struct VecWriter {
    pos: usize,
    pub buf: Vec<u8>,
}

impl VecWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() == self.pos {
            self.buf.extend(buf);
        } else {
            if self.pos + buf.len() <= self.buf.len() {
                self.buf[self.pos..self.pos + buf.len()].copy_from_slice(buf);
            } else {
                let copy_len = self.buf.len() - self.pos;

                self.buf[self.pos..self.pos + copy_len].copy_from_slice(&buf[..copy_len]);

                self.buf.extend(&buf[copy_len..]);
            }
        }

        self.pos += buf.len();

        return Ok(buf.len());
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for VecWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                self.pos = offset as usize;
            }
            SeekFrom::End(offset) => {
                self.pos = self.buf.len() + offset as usize;
            }
            SeekFrom::Current(offset) => {
                self.pos += offset as usize;
            }
        };

        Ok(self.pos as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zip_unzip() {
        let src = vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8, 10u8];

        let compressed = compress_payload(&src).unwrap();

        println!("{}", compressed.len());

        let uncompressed = decompress_payload(&compressed).unwrap();

        println!("{}", uncompressed.len());
    }
}
