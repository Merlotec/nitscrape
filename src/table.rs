use std::{io, fmt, path::Path, fs::File};

use chrono::{DateTime, NaiveDateTime, Utc};

use crate::TweetId;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TweetEntry {
    pub id: TweetId,
    pub timestamp: DateTime<Utc>,
    pub subject: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CsvLayout {
    pub delimiter: u8,
    pub id_idx: usize,
    pub timestamp_idx: usize,
    pub timestamp_format: String,
    pub subject_idx: Option<usize>,
}

pub struct TweetCsvReader<R: io::Read + io::Seek> {
    rdr: csv::Reader<R>,
    layout: CsvLayout,
}

impl<R: io::Read + io::Seek> TweetCsvReader<R> {
    pub fn tweet_entries<'r>(&'r mut self) -> TweetCsvIter<'r, R> {
        TweetCsvIter { iter: self.rdr.records(), layout: &self.layout }
    }
}

impl TweetCsvReader<File> {
    pub fn read_csv<P: AsRef<Path>>(path: P, layout: CsvLayout) -> csv::Result<Self> {
        let rdr = csv::ReaderBuilder::new().delimiter(layout.delimiter).from_path(path)?;
        
        Ok(TweetCsvReader::<File> {
            rdr,
            layout,
        })
    }
}

#[derive(Debug)]
pub enum CsvError {
    DeserializeError,
    IndexOutOfBounds,
    ReadError(csv::Error),
}

impl fmt::Display for CsvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::ReadError(e) => e.fmt(f),
            Self::DeserializeError => write!(f, "Failed to deserialize data"),
            Self::IndexOutOfBounds => write!(f, "Layout index invalid - out of bounds"),
        }

    }
}

pub struct TweetCsvIter<'r, R: io::Read + io::Seek> {
    iter: csv::StringRecordsIter<'r, R>,
    layout: &'r CsvLayout,
}

impl<'r, R: io::Read + io::Seek> Iterator for TweetCsvIter<'r, R> {
    type Item = Result<TweetEntry, CsvError>;

    fn next(&mut self) -> Option<Self::Item> {
        let record =  self.iter.next()?;
        Some(
        match record {
            Ok(r) => {
                let id = {
                    if let Some(id_str) = r.get(self.layout.id_idx) {
                        if let Ok(id) = id_str.parse::<u128>() {
                            id
                        } else {
                            println!("BADu128: {}", id_str);
                            return Some(Err(CsvError::DeserializeError));
                        }
                    } else {
                        return Some(Err(CsvError::IndexOutOfBounds));
                    }
                };

                let timestamp = {
                    if let Some(ts_str) = r.get(self.layout.timestamp_idx) {
                        if let Ok(naive_timestamp) = NaiveDateTime::parse_from_str(&ts_str, &self.layout.timestamp_format) {
                            DateTime::from_utc(naive_timestamp, Utc)
                        } else {
                            println!("BADdate: {}", ts_str);
                            return Some(Err(CsvError::DeserializeError));
                        }
                    } else {
                        return Some(Err(CsvError::IndexOutOfBounds));
                    }
                };

                let subject = {
                    if let Some(subject_idx) = self.layout.subject_idx {
                        if let Some(subj_str) = r.get(subject_idx) {
                            Some(subj_str.to_owned())
                        } else {
                            return Some(Err(CsvError::IndexOutOfBounds));
                        }
                    } else {
                        None
                    }
                };

                Ok(TweetEntry { id, timestamp, subject })
            },
            Err(e) => Err(CsvError::ReadError(e)),
        }
        )

    }
}
