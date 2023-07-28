use std::{io, fmt, path::{Path, PathBuf}, fs::File};

use chrono::{DateTime, NaiveDateTime, Utc};
use reqwest::Client;

use crate::twt::{Tweet, self, TweetId};

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TweetEntry {
    pub pos: Option<u64>,
    pub id: TweetId,
    pub timestamp: Option<DateTime<Utc>>,
    pub subject: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CsvLayout {
    pub delimiter: u8,
    pub id_idx: usize,
    pub timestamp: Option<(usize, String)>,
    pub subject_idx: Option<usize>,
}

impl Default for CsvLayout {
    fn default() -> Self {
        Self {
            delimiter: b',',
            id_idx: 0,
            timestamp: None,
            subject_idx: None,
        }
    }
}

impl CsvLayout {
    pub fn new(delimiter: u8, id_idx: usize, timestamp_idx: usize, timestamp_format: String, subject_idx: Option<usize>) -> Self {
        CsvLayout {
            delimiter,
            id_idx,
            timestamp: Some((timestamp_idx, timestamp_format)),
            subject_idx,
        }
    }

    pub fn without_timestamp(delimiter: u8, id_idx: usize, subject_idx: Option<usize>) -> Self {
        CsvLayout {
            delimiter,
            id_idx,
            timestamp: None,
            subject_idx,
        }
    }
}

pub struct TweetCsvReader<R: io::Read + io::Seek> {
    rdr: csv::Reader<R>,
    layout: CsvLayout,
}

impl<R: io::Read + io::Seek> TweetCsvReader<R> {
    pub fn tweet_entries<'r>(&'r mut self) -> TweetCsvIter<'r, R> {
        TweetCsvIter { iter: self.rdr.records(), layout: &self.layout }
    }

    pub fn count(mut self) -> usize {
        self.rdr.records().count()
    }

    pub fn hydrator<'r, P: AsRef<Path>>(&'r mut self, output: P, cursor: usize) -> csv::Result<CsvHydrator<'r, R>> {
        let mut iter = self.rdr.records();
        if cursor > 0 {
            iter.nth(cursor - 1);
        }
        Ok(CsvHydrator { iter: TweetCsvIter { iter, layout: &self.layout }, writer: csv::WriterBuilder::new().from_path(output)?, cursor })
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

pub fn tweet_reader<P: AsRef<Path>>(path: P) -> csv::Result<csv::Reader<File>> {
    csv::ReaderBuilder::new().from_path(path)
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
                            return Some(Err(CsvError::DeserializeError));
                        }
                    } else {
                        return Some(Err(CsvError::IndexOutOfBounds));
                    }
                };

                let timestamp = {
                    if let Some((ts_idx, ts_format)) = &self.layout.timestamp {
                        if let Some(ts_str) = r.get(*ts_idx) {
                            if let Ok(naive_timestamp) = NaiveDateTime::parse_from_str(&ts_str, ts_format) {
                                Some(DateTime::from_utc(naive_timestamp, Utc))
                            } else {
                                return Some(Err(CsvError::DeserializeError));
                            }
                        } else {
                            return Some(Err(CsvError::IndexOutOfBounds));
                        }
                    } else {
                        None
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

                Ok(TweetEntry { pos: r.position().map(|x| x.record()), id, timestamp, subject })
            },
            Err(e) => Err(CsvError::ReadError(e)),
        }
        )
    }
}

pub fn write_csv<P: AsRef<Path>>(tweets: &[Tweet], path: P) -> csv::Result<()> {
    let mut writer = csv::WriterBuilder::new().from_path(path)?;
    for tweet in tweets {
        writer.serialize(tweet.clone())?;
    }
    Ok(())
}

/// Based on an iterator.
pub struct CsvHydrator<'r, R: io::Read + io::Seek> {
    iter: TweetCsvIter<'r, R>,
    writer: csv::Writer<File>,
    cursor: usize,
}

impl<'r, R: io::Read + io::Seek> CsvHydrator<'r, R> {
    // Hydrates the next batch of tweets.
    pub async fn hydrate_batch(&mut self, clients: &mut impl Iterator<Item=Client>, batch_size: usize, sample_density: usize, attempts: usize) -> csv::Result<()> {
        let batch = (0..batch_size * sample_density).filter_map(|i| {
            if let Some(entry) = self.iter.next() {
                self.cursor += 1;
                if i % sample_density == 0 { Some(entry) } else { None }
            } else {
                None
            }
        });

        let ids = batch.filter_map(|x| x.ok().map(|t| t.id)).collect::<Vec<TweetId>>();
        if !ids.is_empty() {
            let batch: Vec<Tweet> = twt::get_batch(clients, &ids, attempts).await.into_iter().filter_map(Result::ok).collect();
            self.write_csv(&batch)?;
        }
        Ok(())
    }

    fn write_csv(&mut self, tweets: &[Tweet]) -> csv::Result<()> {
        for tweet in tweets {
            self.writer.serialize(tweet.clone())?;
        }
        Ok(())
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }
}

pub fn write_dump(path: impl AsRef<Path>, dump: &[TweetEntry]) -> csv::Result<()> {
    let mut writer: csv::Writer<File> = csv::WriterBuilder::new().from_path(path)?;
    for entry in dump {
        writer.serialize(entry)?;
    }
    Ok(())
}

pub fn read_dump(path: impl AsRef<Path>) -> csv::Result<Vec<TweetEntry>> {
    let mut reader: csv::Reader<File> = csv::ReaderBuilder::new().from_path(path)?;
    Ok(reader.deserialize().filter_map(Result::ok).collect())
}

// pub async fn hydrate_csv<P1: AsRef<Path>, P2: AsRef<Path> + Clone>(input: P1, output: P2, layout: CsvLayout, start: usize, sample_density: usize, attempts: usize) -> csv::Result<()> {
//     assert_ne!(sample_density, 0);
//     // Read batch numher of tweets.
//     let mut reader = TweetCsvReader::read_csv(input, layout)?;
//     let mut entries = reader.tweet_entries();

//     // Get iterator up to start.
//     if start > 0 {
//         let _ = entries.nth(start - 1);
//     }

//     let mut buf: Vec<TweetEntry> = Vec::with_capacity(crate::BATCH_SIZE);

//     loop {
//         if let Some(entry) = entries.nth(sample_density - 1) {
//             if let Ok(entry) = entry {
//                 buf.push(entry)
//             }
//         } else {
//             break;
//         }

//         if buf.len() >= crate::BATCH_SIZE {
//             let ids: Vec<TweetId> = buf.iter().map(|x| x.id).collect();
//             let batch: Vec<Tweet> = crate::get_batch(&ids, attempts).await.into_iter().filter_map(Result::ok).collect();
//             buf.clear();

//             write_csv(&batch, output.clone())?;
//         }
//     }
//     Ok(())
// }