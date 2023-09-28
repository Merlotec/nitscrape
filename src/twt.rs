use chrono::{DateTime, NaiveDateTime, Utc};
use futures::future;
use lazy_static::lazy_static;
pub use lingua::Language;
use lingua::{LanguageDetector, LanguageDetectorBuilder};
use rand::{distributions::Alphanumeric, Rng};
use reqwest::{Client, StatusCode};
use std::{fmt, str::FromStr};

use crate::{net, table::TweetEntry};

pub type Result<T> = std::result::Result<T, TweetError>;

pub type TweetId = u128;

pub const BATCH_SIZE: usize = 200;

pub const JUNK_CHARACTERS: [char; 20] = [
    '(', ')', ',', '\"', '.', ';', ':', '\'', '-', '&', '!', '?', '—', ' ', '–', '|', '“', '”', '‘', '’'
];

lazy_static! {
    /// This is an example for using doc comment attributes
    static ref LANG_DET: LanguageDetector = LanguageDetectorBuilder::from_all_languages().build();
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Tweet {
    pub id: TweetId,
    pub timestamp: DateTime<Utc>,
    pub username: String,
    pub text: String,
    pub lang: Option<Language>,
    pub comments: u32,
    pub retweets: u32,
    pub quotes: u32,
    pub likes: u32,
}

impl fmt::Display for Tweet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{}] {}: \"{}\" \n\tcomments: {}, retweets: {}, quotes: {}, likes: {}",
            self.id,
            &self.timestamp,
            &self.text,
            self.comments,
            self.retweets,
            self.quotes,
            self.likes
        )
    }
}

impl Tweet {
    pub fn processed_words(&self, ignore: &[String]) -> Vec<String> {
        self.text.split_ascii_whitespace().map(|x| x.to_lowercase().replace(&JUNK_CHARACTERS,
        "",)).filter(|x| !ignore.contains(&x)).collect()
    }
}

#[derive(Debug)]
pub enum NitScrapeError {
    WebError(reqwest::Error),
    TooManyAttempts,
    ScraperParseError,
    NoSuchTweet,
    /// Similar to NoSuchTweet, but there is a record of it existing within a thread.
    Unavailable(String),
}

impl fmt::Display for NitScrapeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::WebError(e) => e.fmt(f),
            Self::TooManyAttempts => write!(
                f,
                "Too many attempts at fetching tweet when there is a server error"
            ),
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
    pub scrape_error: NitScrapeError,
    pub tweet_id: TweetId,
}

impl TweetError {
    pub fn new(scrape_error: NitScrapeError, tweet_id: TweetId) -> Self {
        TweetError {
            scrape_error,
            tweet_id,
        }
    }

    pub fn parse_error(tweet_id: TweetId) -> Self {
        TweetError {
            scrape_error: NitScrapeError::ScraperParseError,
            tweet_id,
        }
    }
}

impl fmt::Display for TweetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tweet id: {}; ", &self.tweet_id)?;
        self.scrape_error.fmt(f)
    }
}

pub fn url_for_id(user: &str, id: TweetId) -> String {
    format!("https://nitter.net/{}/status/{}", user, id)
}

pub fn gen_url_for_id(id: TweetId) -> String {
    let n = rand::thread_rng().gen_range(6..12);
    let usr: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect();
    url_for_id(&usr, id)
}

pub fn load_request(tweet_entry: TweetEntry) -> net::LoadRequest<TweetEntry> {
    let req = reqwest::Request::new(
        reqwest::Method::GET,
        reqwest::Url::from_str(&gen_url_for_id(tweet_entry.id)).unwrap(),
    );

    // req.headers_mut().insert("Accept", HeaderValue::from_str("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8").unwrap());
    // req.headers_mut().insert(
    //     "Accept-Encoding",
    //     HeaderValue::from_str("gzip, deflate, br").unwrap(),
    // );
    // req.headers_mut().insert(
    //     "Accept-Language",
    //     HeaderValue::from_str("en-GB,en;q=0.9").unwrap(),
    // );
    // req.headers_mut()
    //     .insert("Cache-Control", HeaderValue::from_str("max-age=0").unwrap());
    // req.headers_mut().insert(
    //     "Sec-Ch-Ua",
    //     HeaderValue::from_str("Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Brave\";v=\"114\"")
    //         .unwrap(),
    // );
    // req.headers_mut()
    //     .insert("Sec-Ch-Ua-Mobile", HeaderValue::from_str("?0").unwrap());
    // req.headers_mut().insert(
    //     "Sec-Ch-Ua-Platform",
    //     HeaderValue::from_str("\"Windows\"").unwrap(),
    // );
    // req.headers_mut()
    //     .insert("Sec-Fetch-Dest", HeaderValue::from_str("document").unwrap());
    // req.headers_mut()
    //     .insert("Sec-Fetch-Mode", HeaderValue::from_str("navigate").unwrap());
    // req.headers_mut()
    //     .insert("Sec-Fetch-Site", HeaderValue::from_str("none").unwrap());
    // req.headers_mut()
    //     .insert("Sec-Fetch-User", HeaderValue::from_str("?1").unwrap());
    // req.headers_mut()
    //     .insert("Sec-Gpc", HeaderValue::from_str("1").unwrap());
    // req.headers_mut().insert(
    //     "Upgrade-Insecure-Requests",
    //     HeaderValue::from_str("1").unwrap(),
    // );
    // req.headers_mut().insert("User-Agent", HeaderValue::from_str("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36").unwrap());

    net::LoadRequest {
        data: tweet_entry,
        req,
    }
}

// pub async fn get_tweets(ids: &[TweetId], attempts: usize) -> Vec<Result<Tweet>> {
//     let mut res = Vec::with_capacity(ids.len());

//     //div into batches.
//     let batches = ids.len() / BATCH_SIZE;

//     for b in 0..batches {
//         let base = b * BATCH_SIZE;
//         res.append(&mut get_batch(&ids[base..base + BATCH_SIZE], attempts).await);
//     }

//     if ids.len() % BATCH_SIZE != 0 {
//         res.append(&mut get_batch(&ids[batches * BATCH_SIZE..], attempts).await);
//     }

//     res
// }

pub async fn get_tweets(
    clients: &mut impl Iterator<Item = Client>,
    ids: &[TweetId],
    attempts: usize,
) -> Vec<Result<Tweet>> {
    let mut res = Vec::with_capacity(ids.len());
    stream_batches(clients, ids, attempts, |mut batch| res.append(&mut batch)).await;
    res
}

pub async fn stream_batches<F>(
    clients: &mut impl Iterator<Item = Client>,
    ids: &[TweetId],
    attempts: usize,
    mut f: F,
) where
    F: FnMut(Vec<Result<Tweet>>),
{
    let batches = ids.len() / BATCH_SIZE;

    for b in 0..batches {
        let base = b * BATCH_SIZE;
        let batch = get_batch(clients, &ids[base..base + BATCH_SIZE], attempts).await;
        f(batch);
    }

    if ids.len() % BATCH_SIZE != 0 {
        let batch = get_batch(clients, &ids[batches * BATCH_SIZE..], attempts).await;
        f(batch);
    }
}

pub async fn get_batch(
    clients: &mut impl Iterator<Item = Client>,
    ids: &[TweetId],
    attempts: usize,
) -> Vec<Result<Tweet>> {
    future::join_all(ids.iter().map(|id| {
        poll_tweet(
            clients.next().expect("Faild to fetch client!"),
            *id,
            attempts,
        )
    }))
    .await
}

pub async fn get_batch_seq(
    clients: &mut impl Iterator<Item = Client>,
    ids: &[TweetId],
    attempts: usize,
) -> Vec<Result<Tweet>> {
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(client) = clients.next() {
            result.push(poll_tweet(client.clone(), *id, attempts).await)
        }
    }
    result
}

pub async fn poll_tweet(client: reqwest::Client, id: TweetId, attempts: usize) -> Result<Tweet> {
    let mut i = 0;
    while i < attempts || attempts == 0 {
        let res = get_tweet(client.clone(), id).await;
        if let Err(TweetError {
            scrape_error: NitScrapeError::WebError(we),
            ..
        }) = &res
        {
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

pub async fn get_tweet(client: reqwest::Client, id: TweetId) -> Result<Tweet> {
    let response = client
        .get(gen_url_for_id(id))
        .send()
        .await
        .map_err(move |e| TweetError::new(e.into(), id))?
        .error_for_status()
        .map_err(move |e| {
            if e.status().unwrap().as_u16() == StatusCode::NOT_FOUND {
                TweetError::new(NitScrapeError::NoSuchTweet, id)
            } else {
                TweetError::new(e.into(), id)
            }
        })?
        .text()
        .await
        .map_err(move |e| TweetError::new(e.into(), id))?;

    parse_nitter(id, response)
}

pub fn parse_nitter(id: TweetId, html: String) -> Result<Tweet> {
    let doc = scraper::Html::parse_document(&html);

    let error_selector = scraper::Selector::parse("div.error-panel>span").unwrap();

    if let Some(error) = doc
        .select(&error_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.inner_html()).to_string())
    {
        if error.trim() == "Invalid tweet ID" {
            return Err(TweetError {
                scrape_error: NitScrapeError::NoSuchTweet,
                tweet_id: id,
            });
        }
    }

    let main_content_selector = scraper::Selector::parse("div.main-tweet").unwrap();
    let text_selector = scraper::Selector::parse("div.tweet-content").unwrap();
    let dt_selector = scraper::Selector::parse("p.tweet-published").unwrap();
    let stat_selector = scraper::Selector::parse("span.tweet-stat>div.icon-container").unwrap();
    let username_selector =
        scraper::Selector::parse("div.fullname-and-username>a.username").unwrap();

    let unavailable_selector = scraper::Selector::parse("div.unavailable-box").unwrap();

    let main_content = doc
        .select(&main_content_selector)
        .next()
        .ok_or(TweetError::parse_error(id))?;

    if let Some(unv) = main_content.select(&unavailable_selector).next() {
        return Err(TweetError {
            scrape_error: NitScrapeError::Unavailable(unv.inner_html()),
            tweet_id: id,
        });
    }

    let text = main_content
        .select(&text_selector)
        .next()
        .map(|x| {
            html_escape::decode_html_entities(&x.text().fold(String::new(), |a, b| a + b))
                .to_string()
        })
        .ok_or(TweetError::parse_error(id))?;

    let dt_str = main_content
        .select(&dt_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.inner_html()).to_string())
        .ok_or(TweetError::parse_error(id))?;

    let timestamp = DateTime::from_utc(
        NaiveDateTime::parse_from_str(&dt_str, "%b %e, %Y · %l:%M %p %Z")
            .map_err(|_| TweetError::parse_error(id))?,
        Utc,
    );

    let username = main_content
        .select(&username_selector)
        .next()
        .map(|x| html_escape::decode_html_entities(&x.inner_html()).to_string())
        .ok_or(TweetError::parse_error(id))?;

    let stats: Vec<Option<u32>> = main_content
        .select(&stat_selector)
        .map(|x| {
            x.text()
                .next()
                .and_then(|txt| Some(txt.trim().replace(",", "").parse::<u32>().unwrap_or(0)))
        })
        .collect();

    let comments = stats.get(0).and_then(ToOwned::to_owned).unwrap_or(0);

    let retweets = stats.get(1).and_then(ToOwned::to_owned).unwrap_or(0);

    let quotes = stats.get(2).and_then(ToOwned::to_owned).unwrap_or(0);

    let likes = stats.get(3).and_then(ToOwned::to_owned).unwrap_or(0);

    let lang = LANG_DET.detect_language_of(&text);

    Ok(Tweet {
        text,
        lang,
        username,
        timestamp,
        comments,
        retweets,
        quotes,
        likes,
        id,
    })
}
