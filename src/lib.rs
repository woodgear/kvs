use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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

/// the magic number ponint out this file is a SKVS(simple-key-val-system) log file
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
struct MagicNumber {
    id: String,
    version: u32,
}

impl MagicNumber {
    /// if this file is a SKVS log file the new mehthod will success
    fn from_file(file: &Path) -> Result<Self> {
        unimplemented!()
    }

    fn new() -> Self {
        MagicNumber {
            id: "SKVS".to_string(),
            version: 1,
        }
    }
    /// to determine is a file is a SKVS log fill we will write some special bytes into this file
    /// the return val of this method is the special bytes
    fn magic() -> Vec<u8> {
        let magic = MagicNumber {
            id: "SKVS".to_string(),
            version: 1,
        };
        unimplemented!()
    }

    // return the version of SKVS log file
    fn version(&self) -> u32 {
        unimplemented!()
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
enum LogRecord {
    Set((String, String)),
    Rm(String),
}

impl LogRecord {
    fn to_bytes(&self) -> Vec<u8> {
        unimplemented!()
    }

    /// given a stream from_bytes try to read itself from it and return the offset
    fn from_bytes(bytes: &[u8]) -> Result<(Self, u64)> {
        unimplemented!()
    }
}

/// respresent the log file
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
struct InDiskLogRecordsV1 {
    magic: MagicNumber,
    recods: Vec<LogRecord>,
}

impl InDiskLogRecordsV1 {
    fn from_file(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

/// struct which represent the log file of v1 SKVS
pub struct InDiskLogV1 {
    file: PathBuf,
    handle: File,
    offset: u64,
}

impl InDiskLogV1 {
    fn open(file: impl Into<PathBuf>) -> Result<Self> {
        let file = file.into();
        if !file.exists() {
            let magic_bytes = MagicNumber::magic();
            fs::write(&file, magic_bytes)?;
        }

        let magic = MagicNumber::from_file(&file)?;
        if magic.version() != 1 {
            return Err(failure::format_err!("we could only use V1 of SKVS"));
        }
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom};
        let mut handle = OpenOptions::new().append(true).open(&file)?;
        // TODO what if log file is damaged
        let offset = handle.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            handle,
            offset,
        })
    }

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
        use std::io::Write;
        let mut bytes = record.to_bytes();
        let bytes_len = bytes.len() as u64;
        self.handle.write_all(&bytes)?;
        self.offset += bytes_len;
        return Ok((self.offset - bytes_len, self.offset));
    }

    /// given a offset, return buffer from log file
    pub fn get(&mut self, start: u64, end: u64) -> Result<Vec<u8>> {
        read_with_offset(&mut self.handle, start, end)
    }
}

pub struct KvStore {
    log: InDiskLogV1,
    /// store the offset of key in log
    in_mem_map: HashMap<String, (u64, u64)>,
}

impl KvStore {
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        unimplemented!()
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }

    /// path: the log of database,we use this log file to reconstruct db states.
    /// log file format
    /// there are some rules when wen pick the format for log
    /// 1. readable
    /// 2. streaming encoding format,we could directly append to the log file and need not to delete the end flag of it,so json is not a valid option
    /// 3. when we read start of reacord we could directly know where is the end of this record so, format should contains the size of it self, just like tcp/ip package
    /// 4. performance etc
    /// so we use msgpack you should check InDiskLogV1 to see how we use it.
    pub fn open(log_dir: impl Into<PathBuf>) -> Result<KvStore> {
        let log_path = log_dir.into().join("log");
        let log = InDiskLogV1::open(&log_path)?;
        Ok(Self {
            log,
            in_mem_map: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    #[test]
    fn test_parse_log_record() {
        use std::io::{BufReader, Seek, SeekFrom};
        #[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
        struct MockJson {
            name: String,
        };
        // let temp_dir = TempDir::new().unwrap();
        // let temp_dir_path = temp_dir.path();
        // let mock_file = temp_dir_path.join("mock");
        // fs::write(&mock_file, mock_json).unwrap();
        // let file = fs::File::open(&mock_file).unwrap();

        // let mut buf_r = BufReader::new(file);
        // println!("offset {:?}", buf_r.seek(SeekFrom::Current(0)));
        // let data: Vec<MockJson> = serde_json::from_reader(buf_r).unwrap();
        // println!("offset {:?}", buf_r.seek(SeekFrom::Current(0)));
        // println!("{:?}", "ok");
    }

    #[test]
    fn test_log_record() {
        // let magic = MagicNumber::magic();
        let temp_dir = TempDir::new().unwrap();
        let temp_dir_path = temp_dir.path();
        let mock_file = temp_dir_path.join("mock");
        let mut stream_log = InDiskLogV1::open(&mock_file).unwrap();
        stream_log.set("a", "1");
        stream_log.set("b", "2");
        stream_log.set("c", "3");
        stream_log.rm("c");
        stream_log.rm("b");

        let static_log_record = InDiskLogRecordsV1::from_file(&mock_file).unwrap();
        assert_eq!(static_log_record, {
            InDiskLogRecordsV1 {
                magic: MagicNumber {
                    id: "SKVS".to_string(),
                    version: 1,
                },
                recods: vec![
                    LogRecord::Set(("a".to_owned(), "1".to_owned())),
                    LogRecord::Set(("b".to_owned(), "2".to_owned())),
                    LogRecord::Set(("c".to_owned(), "3".to_owned())),
                    LogRecord::Rm("c".to_owned()),
                    LogRecord::Rm("d".to_owned()),
                ],
            }
        });
    }
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
