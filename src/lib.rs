use std::fmt;
use futures::future;
use chrono::{DateTime, Utc, NaiveDateTime};
use reqwest::StatusCode;

pub mod table;

pub type Result<T> = std::result::Result<T, TweetError>;

pub type TweetId = u128;

const BATCH_SIZE: usize = 25;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tweet {
    pub text: String,
    pub timestamp: DateTime<Utc>,
    pub comments: u32,
    pub retweets: u32,
    pub quotes: u32,
    pub likes: u32,
    pub id: TweetId,
}

impl fmt::Display for Tweet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{}] {}: \"{}\" \n\tcomments: {}, retweets: {}, quotes: {}, likes: {}", self.id, &self.timestamp, &self.text, self.comments, self.retweets, self.quotes, self.likes)
    }
}

#[derive(Debug)]
pub enum NitScrapeError {
    WebError(reqwest::Error),
    TooManyAttempts,
    ScraperParseError,
    NoSuchTweet,
    Unavailable(String),
}

impl fmt::Display for NitScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::WebError(e) => e.fmt(f),
            Self::TooManyAttempts => write!(f, "Too many attempts at fetching tweet when there is a server error"),
            Self::ScraperParseError => write!(f, "Failed to scrape nitter page"),
            Self::NoSuchTweet => write!(f, "No such tweet with the given id"),
            Self::Unavailable(reason) => write!(f, "Tweet unavailable: {}", reason),
        }

    }
}

impl From<reqwest::Error> for NitScrapeError {
    fn from(value: reqwest::Error) -> Self {
        Self::WebError(value)
    }
}

#[derive(Debug)]
pub struct TweetError {
    scrape_error: NitScrapeError,
    tweet_id: TweetId,
}

impl TweetError {
    pub fn new(scrape_error: NitScrapeError, tweet_id: TweetId) -> Self {
        TweetError { scrape_error, tweet_id }
    }

    pub fn parse_error(tweet_id: TweetId) -> Self {
        TweetError { scrape_error: NitScrapeError::ScraperParseError, tweet_id }
    }
}

impl fmt::Display for TweetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tweet id: {}; ", &self.tweet_id)?;
        self.scrape_error.fmt(f)
    }
}


pub fn url_for_id(id: TweetId) -> String {
    format!("https://nitter.net/user/status/{}", id)
}

pub async fn get_tweets(ids: &[TweetId], attempts: usize) -> Vec<Result<Tweet>> {
    let mut res = Vec::with_capacity(ids.len());

    //div into batches.
    let batches = ids.len() / BATCH_SIZE;

    for b in 0..batches {
        let base = b * BATCH_SIZE;
        res.append(&mut get_batch(&ids[base..base + BATCH_SIZE], attempts).await);
    }

    if ids.len() % BATCH_SIZE != 0 {
        res.append(&mut get_batch(&ids[batches * BATCH_SIZE..], attempts).await);
    }

    res
}

pub async fn get_batch(ids: &[TweetId], attempts: usize) -> Vec<Result<Tweet>> {
    future::join_all(ids.iter().map(|id| poll_tweet(*id, attempts))).await
}

pub async fn get_batch_seq(ids: &[TweetId], attempts: usize) -> Vec<Result<Tweet>> {
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        result.push(poll_tweet(*id, attempts).await)
    }
    result
}

pub async fn poll_tweet(id: TweetId, attempts: usize) -> Result<Tweet> {
    let mut i = 0;
    while i < attempts || attempts == 0 {
        let res = get_tweet(id).await;
        if let Err(TweetError { scrape_error: NitScrapeError::WebError(we), .. }) = &res {
            if let Some(status) = we.status() {
                if status.is_server_error() {
                    i += 1;
                    continue;
                }
            }
        }
        return res;
    }
    Err(TweetError::new(NitScrapeError::TooManyAttempts, id))
}

pub async fn get_tweet(id: TweetId) -> Result<Tweet> {
    let response = reqwest::get(
        url_for_id(id),
    )
    .await.map_err(move |e| TweetError::new(e.into(), id))?
    .error_for_status().map_err(move |e| 
        if e.status().unwrap().as_u16() == StatusCode::NOT_FOUND {
            TweetError::new(NitScrapeError::NoSuchTweet, id)
        } else {
            TweetError::new(e.into(), id) 
        }
    )?
    .text()
    .await.map_err(move |e| TweetError::new(e.into(), id))?;

    let doc = scraper::Html::parse_document(&response);

    let error_selector = scraper::Selector::parse("div.error-panel>span").unwrap();

    if let Some(error) = doc
        .select(&error_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.inner_html()).to_string()) {
        if error.trim() == "Invalid tweet ID" {
            return Err(TweetError { scrape_error: NitScrapeError::NoSuchTweet, tweet_id: id })
        }
    }

    let main_content_selector = scraper::Selector::parse("div.main-tweet").unwrap();
    let text_selector = scraper::Selector::parse("div.tweet-content").unwrap();
    let dt_selector = scraper::Selector::parse("p.tweet-published").unwrap();
    let stat_selector = scraper::Selector::parse("span.tweet-stat>div.icon-container").unwrap();

    let unavailable_selector = scraper::Selector::parse("div.unavailable-box").unwrap();

    let main_content = doc
        .select(&main_content_selector)
        .next()
        .ok_or(TweetError::parse_error(id))?;

    if let Some(unv) = main_content
        .select(&unavailable_selector)
        .next()
    {
        return Err(TweetError { scrape_error: NitScrapeError::Unavailable(unv.inner_html()), tweet_id: id })    
    }

    let text = main_content
        .select(&text_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.text().fold(String::new(), |a, b| a + b)).to_string())
        .ok_or(TweetError::parse_error(id))?;

    let dt_str = main_content
        .select(&dt_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.inner_html()).to_string())
        .ok_or(TweetError::parse_error(id))?;

    let timestamp = DateTime::from_utc(
        NaiveDateTime::parse_from_str(&dt_str, "%b %e, %Y · %l:%M %p %Z")
            .map_err(|_|TweetError::parse_error(id))?, 
        Utc
    );

    let stats: Vec<Option<u32>> = main_content
        .select(&stat_selector)
        .map(|x|x.text().next().and_then(|txt| Some(txt.trim().replace(",", "").parse::<u32>().unwrap())))
        .collect();

    let comments = stats.get(0).and_then(ToOwned::to_owned)
        .unwrap_or(0);

    let retweets = stats.get(1).and_then(ToOwned::to_owned)
        .unwrap_or(0);

    let quotes = stats.get(2).and_then(ToOwned::to_owned)
        .unwrap_or(0);

    let likes = stats.get(3).and_then(ToOwned::to_owned)
        .unwrap_or(0);
 
    Ok(
        Tweet {
            text,
            timestamp,
            comments,
            retweets,
            quotes,
            likes,
            id,
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scrape_tweet() {
        let tweet = get_tweet(1278368973893951489).await.unwrap();
        println!("{}", tweet);
    }

    #[tokio::test]
    async fn batch() {
        let ids = vec![20; 30];

        let tweets = get_tweets(&ids, 0).await;

        for (i, tweet) in tweets.iter().enumerate() {
            println!("{}: {}", i, tweet.as_ref().unwrap());
        }
    }

    #[tokio::test]
    async fn large_csv() {
        let mut csv = table::TweetCsvReader::read_csv("C:\\Users\\ncbmk\\dev\\laidlaw\\tsa\\uselection_tweets_1jul_11nov.csv", table::CsvLayout { delimiter: b';', id_idx: 6, timestamp_idx: 0, timestamp_format: "%D %l:%M %p".to_owned(), subject_idx: Some(5) }).unwrap();
        let mut entries = csv.tweet_entries();
        // Get 50 tweets
        for i in 0..50 {
            if let Some(entry) = entries.next() {
                match entry {
                    Ok(entry) => {
                        match get_tweet(entry.id).await {
                            Ok(tweet) => println!("{}: {}", i, tweet),
                            Err(e) => println!("{}: Failed to get tweet with id, {}", i, e)
                        }
                    },
                    Err(e) => println!("Failed to get tweet from file: {}", e),
                }
            }
        }
    }
}
