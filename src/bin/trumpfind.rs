use anyhow::{Context, Result};
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};

const TRUMP_START: u64 = 215_041_381_749_581;
const TRUMP_END: u64 = 215_041_481_749_581;

fn u64_field(v: &Value, key: &str) -> Option<u64> {
    v.get(key)?.as_u64()
}

fn str_field<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
    v.get(key)?.as_str()
}

async fn ord_status(http: &reqwest::Client, ord_base: &str) -> Result<Value> {
    let url = format!("{}/status", ord_base.trim_end_matches('/'));
    let resp = http.get(url).header("Accept", "application/json").send().await?;
    Ok(resp.error_for_status()?.json().await?)
}

async fn ord_inscription_by_number(
    http: &reqwest::Client,
    ord_base: &str,
    n: u64,
) -> Result<Option<Value>> {
    let url = format!("{}/inscription/{}", ord_base.trim_end_matches('/'), n);
    let resp = match http.get(url).header("Accept", "application/json").send().await {
    Ok(r) => r,
    Err(e) if e.is_timeout() => return Ok(None),
    Err(e) => return Err(e.into()),
};

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    Ok(Some(resp.error_for_status()?.json().await?))
}

fn append_jsonl(path: &str, v: &Value) -> Result<()> {
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut f, v)?;
    f.write_all(b"\n")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let ord_base = env::var("TRUMPFIND_ORD_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8083".to_string());
    let concurrency: usize = env::var("TRUMPFIND_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let batch: u64 = env::var("TRUMPFIND_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let start_n: u64 = env::var("TRUMPFIND_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let hits_path = env::var("TRUMPFIND_HITS_PATH")
        .unwrap_or_else(|_| "/var/lib/trumpfind/trump_hits.jsonl".to_string());
    let first_path = env::var("TRUMPFIND_FIRST_PATH")
        .unwrap_or_else(|_| "/var/lib/trumpfind/trump_first.json".to_string());

    // Ensure hits file exists so tail -F works immediately
    let _ = OpenOptions::new().create(true).append(true).open(&hits_path);

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(concurrency)
        .build()
        .context("build http client")?;

    let sem = Arc::new(Semaphore::new(concurrency));

    let mut next_n = start_n;
    let mut trump_total: u64 = 0;
    let mut first_trump: Option<u64> = None;

    loop {
        let st = ord_status(&http, &ord_base).await.context("fetch ord /status")?;
        let ord_inscriptions = st.get("inscriptions").and_then(|v| v.as_u64()).unwrap_or(0);

        if ord_inscriptions == 0 {
            eprintln!("ord reports 0 inscriptions; waiting...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let max_n = ord_inscriptions.saturating_sub(1);

        if next_n > max_n {
            eprintln!(
                "caught up to ord tip (next_n={} > max_n={}); first_trump={:?}; trump_total={}; sleeping...",
                next_n, max_n, first_trump, trump_total
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        let end_n = (next_n + batch - 1).min(max_n);
        let started = Instant::now();

        let mut tasks = Vec::with_capacity((end_n - next_n + 1) as usize);
        for n in next_n..=end_n {
            let sem = sem.clone();
            let http = http.clone();
            let ord_base = ord_base.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await?;
                let v = ord_inscription_by_number(&http, &ord_base, n).await?;
                Ok::<(u64, Option<Value>), anyhow::Error>((n, v))
            }));
        }

        let mut scanned: u64 = 0;

        for t in tasks {
            let (n, opt) = t.await.context("join task")??;
            scanned += 1;

            let Some(v) = opt else { continue; };
            let sat = match u64_field(&v, "sat") {
                Some(x) => x,
                None => continue,
            };

            if sat >= TRUMP_START && sat <= TRUMP_END {
                trump_total += 1;

                let hit = json!({
                    "number": n,
                    "id": str_field(&v, "id"),
                    "sat": sat,
                    "satpoint": v.get("satpoint"),
                    "height": v.get("height"),
                    "timestamp": v.get("timestamp"),
                    "trump_total_so_far": trump_total
                });

                // Persist every hit
                if let Err(e) = append_jsonl(&hits_path, &hit) {
                    eprintln!("WARN: failed to append hit: {e:#}");
                }

                // Visible one-line log per hit
                eprintln!(
                    "HIT trump_total={} number={} id={} sat={}",
                    trump_total,
                    n,
                    str_field(&v, "id").unwrap_or(""),
                    sat
                );

                // Persist first hit once
                if first_trump.is_none() {
                    first_trump = Some(n);
                    let first = json!({ "first_number": n, "hit": hit });
                    let _ = fs::write(&first_path, serde_json::to_vec_pretty(&first)?);
                }
            }
        }

        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        let rate = (scanned as f64) / elapsed;

        eprintln!(
            "scanned [{}..={}]; rate={:.1}/s; next_n={} max_n={}; first_trump={:?}; trump_total={}",
            next_n, end_n, rate, end_n + 1, max_n, first_trump, trump_total
        );

        next_n = end_n + 1;
    }
}
