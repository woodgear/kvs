use context_attribute::context;
use failure::ResultExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
pub type Result<T> = std::result::Result<T, failure::Error>;

/// give a offset of file it will return the buffer of this offset
pub fn read_with_offset(file: &mut File, start: u64, end: u64) -> Result<Vec<u8>> {
    use std::fs::File;
    use std::io::{self, Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(start))?;
    let mut buffer = Vec::new();
    {
        let reference = file.by_ref();

        // read at most 5 bytes
        reference.take(end - start).read_to_end(&mut buffer)?;
    }
    return Ok(buffer);
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
enum LogRecord {
    Set((String, String)),
    Rm(String),
}

impl LogRecord {
    fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buff = vec![];
        let doc = bson::to_bson(&self)?;
        let doc = doc
            .as_document()
            .ok_or(failure::format_err!("bson is not doc"))?;
        bson::encode_document(&mut buff, doc)?;
        return Ok(buff);
    }

    /// given a stream from_bytes try to read itself from it and return the offset
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        use std::io::Cursor;
        decode_bson(&mut Cursor::new(bytes))?.ok_or(failure::err_msg("read the end of bytes"))
    }
}

use std::io::Read;
fn decode_bson<T, R>(r: &mut R) -> Result<Option<T>>
where
    T: Deserialize<'static>,
    R: Read + ?Sized,
{
    let doc = bson::decode_document(r);
    if let Err(bson::DecoderError::EndOfStream) = doc {
        return Ok(None);
    }
    let doc = doc?;
    let ret: T = bson::from_bson(bson::Bson::Document(doc))?;
    Ok(Some(ret))
}

/// struct which represent the log file of v1 SKVS
pub struct InDiskLogV1 {
    file: PathBuf,
}

// simple record log
impl InDiskLogV1 {
    /// write set command into log return the offset(start_offset,end_offset) of the val of this set command
    pub fn set(&mut self, key: &str, val: &str) -> Result<(u64, u64)> {
        self.append_record(LogRecord::Set((key.to_owned(), val.to_owned())))
    }

    /// write rm command into log
    pub fn rm(&mut self, key: &str) -> Result<()> {
        self.append_record(LogRecord::Rm(key.to_owned()))?;
        Ok(())
    }

    fn append_record(&mut self, record: LogRecord) -> Result<(u64, u64)> {
        use std::io::{Seek, SeekFrom, Write};
        let mut handle = fs::OpenOptions::new().append(true).open(&self.file)?;
        let offset = handle.seek(SeekFrom::End(0))?;
        let bytes = record.to_bytes()?;
        let bytes_len = bytes.len() as u64;
        handle.write_all(&bytes)?;
        return Ok((offset, offset + bytes_len));
    }

    /// given a offset, return buffer from log file
    pub fn get(&mut self, start: u64, end: u64) -> Result<Vec<u8>> {
        let mut handle = fs::File::open(&self.file)?;
        read_with_offset(&mut handle, start, end)
    }
}

pub struct KvStore {
    log: InDiskLogV1,
    /// store the offset of key in log
    in_mem_map: HashMap<String, (u64, u64)>,
}

impl KvStore {
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let offset = self.log.append_record(LogRecord::Set((key.clone(), val)))?;
        self.in_mem_map.insert(key, offset);
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(offset) = self.in_mem_map.get(&key) {
            let buffer = self.log.get(offset.0, offset.1)?;
            let record = LogRecord::from_bytes(&buffer)?;
            if let LogRecord::Set((key, val)) = record {
                return Ok(Some(val.clone()));
            } else {
                return Err(failure::format_err!(
                    "expect read set record but read a {:?}",
                    record
                ));
            }
        }
        return Ok(None);
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.in_mem_map.remove(&key).is_none() {
            return Err(failure::format_err!("{} not exist", key));
        }
        self.log.rm(&key)?;
        Ok(())
    }

    /// path: the log of database,we use this log file to reconstruct db states.
    /// log file format
    /// streaming encoding format,we could directly append to the log file and need not to delete the end flag of it,so json is not a valid option
    /// it seems that the only option is bson
    pub fn open(log_dir: impl Into<PathBuf>) -> Result<KvStore> {
        use std::io::{Cursor, Seek, SeekFrom};

        let file = log_dir.into().join("skvs.db");
        let mut in_mem_map: HashMap<String, (u64, u64)> = HashMap::new();

        if !file.exists() {
            fs::File::create(&file).context("try to create log fail")?;
        }
        let buff = fs::read(&file)?;
        let mut cur = Cursor::new(&buff[..]);

        if buff.is_empty() {
            return Ok(Self {
                in_mem_map,
                log: InDiskLogV1 { file },
            });
        }

        loop {
            let start = cur.position();
            let record = {
                if let Some(r) = decode_bson::<LogRecord, _>(&mut cur)? {
                    r
                } else {
                    break;
                }
            };
            let end = cur.position();
            match record {
                LogRecord::Set((k, _v)) => {
                    in_mem_map.insert(k, (start, end));
                }
                LogRecord::Rm(key) => {
                    in_mem_map.remove(&key);
                }
            };
            if end == buff.len() as u64 {
                break;
            }
        }

        Ok(Self {
            in_mem_map,
            log: InDiskLogV1 { file },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    #[test]
    fn test_read_offset() {
        let data = vec![0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9];
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_path = temp_dir.path();
        let mock_file = temp_dir_path.join("mock");
        std::fs::write(&mock_file, &data).unwrap();
        let mut mf = std::fs::File::open(&mock_file).unwrap();
        let out = read_with_offset(&mut mf, 0, 10).unwrap();
        assert_eq!(out, &data[0..10]);
        let out = read_with_offset(&mut mf, 3, 7).unwrap();
        assert_eq!(out, &data[3..7]);
    }
}
