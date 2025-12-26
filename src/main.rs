use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    str::FromStr,
    time::Duration,
};
use tokio::time::sleep;
use tracing::{error, info, warn};

const INSCRIPTION_START_HEIGHT: u32 = 767_430;

#[derive(Clone)]
struct App {
    cfg: Config,
    db_path: String,
    http: reqwest::Client,
}

#[derive(Clone, Debug)]
struct Config {
    bind_addr: SocketAddr,
    ord_base_url: String,
    poll_interval: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let bind_addr =
            std::env::var("RARERANKD_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8090".to_string());
        let bind_addr = SocketAddr::from_str(&bind_addr)
            .context("RARERANKD_BIND_ADDR must be like 127.0.0.1:8090")?;

        let ord_base_url =
            std::env::var("RARERANKD_ORD_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8083".to_string());

        let poll_interval_secs: u64 = std::env::var("RARERANKD_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        Ok(Self {
            bind_addr,
            ord_base_url,
            poll_interval: Duration::from_secs(poll_interval_secs),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CategoryId {
    Block9,
    Palindrome,
    TrumpCasascius,
}

impl CategoryId {
    fn as_str(self) -> &'static str {
        match self {
            CategoryId::Block9 => "block9",
            CategoryId::Palindrome => "palindrome",
            CategoryId::TrumpCasascius => "trump_casascius",
        }
    }
}

impl FromStr for CategoryId {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "block9" => Self::Block9,
            "palindrome" => Self::Palindrome,
            "trump_casascius" => Self::TrumpCasascius,
            _ => return Err(anyhow!("unknown category_id `{s}`")),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct OrdStatus {
    height: u32,
    inscriptions: u64,
    sat_index: bool,
    inscription_index: bool,
}

#[derive(Debug, Serialize)]
struct DaemonStatus {
    ord: Option<OrdStatus>,
    next_inscription_number: u64,
    category_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct RareRankResponse {
    category_id: String,
    inscription_id: String,
    rank: u64,
    total: u64,
}

fn db_open(db_path: &str) -> Result<Connection> {
    Connection::open(db_path).context("open sqlite db")
}

fn db_init(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS meta(
  k TEXT PRIMARY KEY NOT NULL,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS category_counts(
  category_id TEXT PRIMARY KEY NOT NULL,
  count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS inscription_ranks(
  inscription_id TEXT NOT NULL,
  category_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  PRIMARY KEY(inscription_id, category_id)
);

CREATE TABLE IF NOT EXISTS processed_inscriptions(
  inscription_number INTEGER PRIMARY KEY NOT NULL,
  inscription_id TEXT NOT NULL
);
"#,
    )?;

    for cat in [CategoryId::Block9, CategoryId::Palindrome, CategoryId::TrumpCasascius] {
        conn.execute(
            "INSERT OR IGNORE INTO category_counts(category_id, count) VALUES (?1, 0)",
            params![cat.as_str()],
        )?;
    }

    conn.execute(
        "INSERT OR IGNORE INTO meta(k, v) VALUES ('next_inscription_number', '0')",
        [],
    )?;

    Ok(())
}

fn db_get_next_inscription_number(conn: &Connection) -> Result<u64> {
    let v: String = conn.query_row(
        "SELECT v FROM meta WHERE k = 'next_inscription_number'",
        [],
        |row| row.get(0),
    )?;
    Ok(v.parse::<u64>().context("meta.next_inscription_number must be u64")?)
}

fn db_set_next_inscription_number(conn: &Connection, n: u64) -> Result<()> {
    conn.execute(
        "UPDATE meta SET v = ?1 WHERE k = 'next_inscription_number'",
        params![n.to_string()],
    )?;
    Ok(())
}

fn db_get_category_counts(conn: &Connection) -> Result<BTreeMap<String, u64>> {
    let mut stmt = conn.prepare("SELECT category_id, count FROM category_counts ORDER BY category_id ASC")?;
    let rows = stmt.query_map([], |row| {
        let k: String = row.get(0)?;
        let v: i64 = row.get(1)?;
        Ok((k, v as u64))
    })?;

    let mut out = BTreeMap::new();
    for r in rows {
        let (k, v) = r?;
        out.insert(k, v);
    }
    Ok(out)
}

fn db_get_rank(conn: &Connection, category_id: &str, inscription_id: &str) -> Result<Option<u64>> {
    let mut stmt = conn.prepare(
        "SELECT rank FROM inscription_ranks WHERE category_id = ?1 AND inscription_id = ?2 LIMIT 1",
    )?;
    let mut rows = stmt.query(params![category_id, inscription_id])?;
    if let Some(row) = rows.next()? {
        let rank: i64 = row.get(0)?;
        Ok(Some(rank as u64))
    } else {
        Ok(None)
    }
}

fn db_increment_and_record_rank(conn: &mut Connection, category_id: &str, inscription_id: &str) -> Result<u64> {
    if let Some(existing) = db_get_rank(conn, category_id, inscription_id)? {
        return Ok(existing);
    }

    let tx = conn.transaction()?;
    let current: i64 = tx.query_row(
        "SELECT count FROM category_counts WHERE category_id = ?1",
        params![category_id],
        |row| row.get(0),
    )?;
    let next = current + 1;

    tx.execute(
        "UPDATE category_counts SET count = ?1 WHERE category_id = ?2",
        params![next, category_id],
    )?;
    tx.execute(
        "INSERT INTO inscription_ranks(inscription_id, category_id, rank) VALUES (?1, ?2, ?3)",
        params![inscription_id, category_id, next],
    )?;

    tx.commit()?;
    Ok(next as u64)
}

fn db_mark_processed(conn: &Connection, inscription_number: u64, inscription_id: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO processed_inscriptions(inscription_number, inscription_id) VALUES (?1, ?2)",
        params![inscription_number as i64, inscription_id],
    )?;
    Ok(())
}

fn block9_range() -> (u64, u64) {
    (45_000_000_000, 49_999_999_999)
}

fn is_palindrome_decimal(n: u64) -> bool {
    let s = n.to_string();
    s.chars().eq(s.chars().rev())
}

fn categories_for_sat(sat: u64) -> Vec<CategoryId> {
    let mut out = Vec::new();

    let (b9_start, b9_end) = block9_range();
    if (b9_start..=b9_end).contains(&sat) {
        out.push(CategoryId::Block9);
    }

    if is_palindrome_decimal(sat) {
        out.push(CategoryId::Palindrome);
    }

    if (215_041_381_749_581u64..215_041_481_749_581u64).contains(&sat) {
        out.push(CategoryId::TrumpCasascius);
    }

    out
}

async fn ord_status(app: &App) -> Result<OrdStatus> {
    let url = format!("{}/status", app.cfg.ord_base_url.trim_end_matches('/'));
    let v: Value = app
        .http
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(OrdStatus {
        height: v.get("height").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("ord /status missing height"))? as u32,
        inscriptions: v.get("inscriptions").and_then(|v| v.as_u64()).ok_or_else(|| anyhow!("ord /status missing inscriptions"))?,
        sat_index: v.get("sat_index").and_then(|v| v.as_bool()).ok_or_else(|| anyhow!("ord /status missing sat_index"))?,
        inscription_index: v.get("inscription_index").and_then(|v| v.as_bool()).ok_or_else(|| anyhow!("ord /status missing inscription_index"))?,
    })
}

async fn ord_inscription_by_number(app: &App, n: u64) -> Result<Option<Value>> {
    let url = format!("{}/inscription/{}", app.cfg.ord_base_url.trim_end_matches('/'), n);
    let v: Value = app
        .http
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(if v.is_null() { None } else { Some(v) })
}

fn extract_inscription_id_and_sat(v: &Value) -> Result<(String, u64)> {
    let inscription_id = v
        .get("id")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("inscription_id").and_then(|x| x.as_str()))
        .ok_or_else(|| anyhow!("inscription JSON missing `id`"))?
        .to_string();

    let sat = v
        .get("sat")
        .and_then(|x| x.as_u64())
        .or_else(|| v.get("sat_number").and_then(|x| x.as_u64()))
        .or_else(|| v.get("satpoint").and_then(|sp| sp.get("sat")).and_then(|x| x.as_u64()))
        .ok_or_else(|| anyhow!("inscription JSON missing sat number field (sat/sat_number/satpoint.sat)"))?;

    Ok((inscription_id, sat))
}

async fn ingest_loop(app: App) {
    loop {
        let st = match ord_status(&app).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "ord status not available yet; retrying");
                sleep(app.cfg.poll_interval).await;
                continue;
            }
        };

        if !st.sat_index || !st.inscription_index {
            warn!(sat_index = st.sat_index, inscription_index = st.inscription_index, "ord not configured for rareRank inputs yet");
            sleep(app.cfg.poll_interval).await;
            continue;
        }

        if st.height < INSCRIPTION_START_HEIGHT {
            info!(ord_height = st.height, "ord still reindexing below inscription start; waiting");
            sleep(app.cfg.poll_interval).await;
            continue;
        }

        let mut conn = match db_open(&app.db_path) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "db open failed");
                sleep(app.cfg.poll_interval).await;
                continue;
            }
        };

        let next_n = match db_get_next_inscription_number(&conn) {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "failed to read cursor");
                sleep(app.cfg.poll_interval).await;
                continue;
            }
        };

        let payload = match ord_inscription_by_number(&app, next_n).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                sleep(app.cfg.poll_interval).await;
                continue;
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch inscription by number; retrying");
                sleep(app.cfg.poll_interval).await;
                continue;
            }
        };

        let (inscription_id, sat) = match extract_inscription_id_and_sat(&payload) {
            Ok(x) => x,
            Err(e) => {
                error!(error = %e, "inscription JSON schema unexpected; not advancing cursor");
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        if let Err(e) = db_mark_processed(&conn, next_n, &inscription_id) {
            error!(error = %e, "failed to mark processed");
            sleep(app.cfg.poll_interval).await;
            continue;
        }

        for cat in categories_for_sat(sat) {
            if let Err(e) = db_increment_and_record_rank(&mut conn, cat.as_str(), &inscription_id) {
                error!(error = %e, category = cat.as_str(), "failed to record rank");
                sleep(app.cfg.poll_interval).await;
                continue;
            }
        }

        if let Err(e) = db_set_next_inscription_number(&conn, next_n + 1) {
            error!(error = %e, "failed to advance cursor");
            sleep(app.cfg.poll_interval).await;
            continue;
        }

        sleep(Duration::from_millis(50)).await;
    }
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn status(State(app): State<App>) -> impl IntoResponse {
    let ord = ord_status(&app).await.ok();

    let mut conn = db_open(&app.db_path).ok();
    let (next, counts) = if let Some(ref conn) = conn {
        (
            db_get_next_inscription_number(conn).unwrap_or(0),
            db_get_category_counts(conn).unwrap_or_default(),
        )
    } else {
        (0, BTreeMap::new())
    };

    Json(DaemonStatus { ord, next_inscription_number: next, category_counts: counts })
}

async fn get_rarerank(
    State(app): State<App>,
    Path((category_id, inscription_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let cat = match CategoryId::from_str(&category_id) {
        Ok(c) => c,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let conn = match db_open(&app.db_path) {
        Ok(c) => c,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let rank = match db_get_rank(&conn, cat.as_str(), &inscription_id) {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let total = match db_get_category_counts(&conn) {
        Ok(m) => m.get(cat.as_str()).copied().unwrap_or(0),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    match rank {
        Some(rank) => Json(RareRankResponse { category_id: cat.as_str().to_string(), inscription_id, rank, total }).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cfg = Config::from_env()?;
    let db_path = std::env::var("RARERANKD_DB_PATH").unwrap_or_else(|_| "/var/lib/rarerankd/rarerankd.sqlite".to_string());

    std::fs::create_dir_all(std::path::Path::new(&db_path).parent().unwrap()).ok();

    {
        let conn = db_open(&db_path)?;
        db_init(&conn)?;
    }

    let app = App {
        cfg: cfg.clone(),
        db_path: db_path.clone(),
        http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
    };

    let ingest_app = app.clone();
    tokio::spawn(async move { ingest_loop(ingest_app).await });

    let router = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/rarerank/:category_id/inscription/:inscription_id", get(get_rarerank))
        .with_state(app);

    info!(bind_addr = %cfg.bind_addr, "rarerankd listening");
    let listener = tokio::net::TcpListener::bind(cfg.bind_addr).await?;
    axum::serve(listener, router.into_make_service()).await?;
    Ok(())
}
