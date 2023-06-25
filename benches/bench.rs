
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn get_tweets_benchmark(c: &mut Criterion) {
    c.bench_function("tweets 100", |b| b.iter(|| nitscrape::get_tweet(1672611045196587009)));
}

criterion_group!(benches, get_tweets_benchmark);
criterion_main!(benches);