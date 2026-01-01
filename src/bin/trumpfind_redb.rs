use anyhow::{anyhow, Context, Result};
use redb::{Database, ReadableDatabase, TableDefinition};
use serde_json::json;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

const INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER: TableDefinition<i32, u32> =
  TableDefinition::new("INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER");

type InscriptionIdValue = (u128, u128, u32);
type InscriptionEntryValue = (
  u16,                // charms
  u64,                // fee
  u32,                // height
  InscriptionIdValue, // inscription id
  i32,                // inscription number
  Vec<u32>,           // parents
  Option<u64>,        // sat
  u32,                // sequence number
  u32,                // timestamp
);

const SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY: TableDefinition<u32, InscriptionEntryValue> =
  TableDefinition::new("SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY");

fn hex32(bytes: [u8; 32]) -> String {
  const HEX: &[u8; 16] = b"0123456789abcdef";
  let mut out = vec![0u8; 64];
  for (i, b) in bytes.iter().enumerate() {
    out[i * 2] = HEX[(b >> 4) as usize];
    out[i * 2 + 1] = HEX[(b & 0x0f) as usize];
  }
  String::from_utf8(out).unwrap()
}

fn inscription_id_string((head, tail, index): InscriptionIdValue) -> String {
  let head = head.to_le_bytes();
  let tail = tail.to_le_bytes();
  let mut bytes = [0u8; 32];
  bytes[..16].copy_from_slice(&head);
  bytes[16..].copy_from_slice(&tail);
  bytes.reverse();
  format!("{}i{}", hex32(bytes), index)
}

fn append_jsonl(path: &str, value: &serde_json::Value) -> Result<()> {
  if let Some(parent) = Path::new(path).parent() {
    std::fs::create_dir_all(parent).with_context(|| format!("create_dir_all {parent:?}"))?;
  }
  let mut f = OpenOptions::new()
    .create(true)
    .append(true)
    .open(path)
    .with_context(|| format!("open {path} for append"))?;
  writeln!(f, "{}", value)?;
  Ok(())
}

fn getenv_u64(name: &str) -> Result<u64> {
  env::var(name)
    .with_context(|| format!("missing env {name}"))?
    .parse::<u64>()
    .with_context(|| format!("invalid u64 in env {name}"))
}

fn getenv_i32(name: &str, default: i32) -> i32 {
  env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() -> Result<()> {
  let db_path = env::var("TRUMPFIND_DB_PATH").unwrap_or_else(|_| "/var/lib/ord/index.redb".into());
  let start_n = getenv_i32("TRUMPFIND_START", 0);

  let trump_sat_start = getenv_u64("TRUMPFIND_TRUMP_SAT_START")?;
  let trump_sat_end_exclusive = getenv_u64("TRUMPFIND_TRUMP_SAT_END_EXCLUSIVE")?;
  if trump_sat_end_exclusive <= trump_sat_start {
    return Err(anyhow!(
      "bad Trump sat range: start={} end_exclusive={}",
      trump_sat_start,
      trump_sat_end_exclusive
    ));
  }

  let hits_path =
    env::var("TRUMPFIND_HITS_PATH").unwrap_or_else(|_| "/var/lib/trumpfind/trump_hits.jsonl".into());
  let first_path =
    env::var("TRUMPFIND_FIRST_PATH").unwrap_or_else(|_| "/var/lib/trumpfind/trump_first.json".into());

  eprintln!("trumpfind_redb: db_path={db_path}");
  eprintln!("trumpfind_redb: start inscription_number={start_n}");
  eprintln!("trumpfind_redb: trump sat range [{trump_sat_start}, {trump_sat_end_exclusive})");

  let db = Database::open(&db_path).with_context(|| format!("open redb {db_path}"))?;
  let rtx = db.begin_read()?;

  let n_to_seq = rtx.open_table(INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)?;
  let seq_to_entry = rtx.open_table(SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)?;

  let mut scanned: u64 = 0;
  let mut hits: u64 = 0;
  let mut first_written = Path::new(&first_path).exists();

  for item in n_to_seq.range(start_n..)? {
    let (n_guard, seq_guard): (redb::AccessGuard<'_, i32>, redb::AccessGuard<'_, u32>) = item?;

    let n: i32 = n_guard.value();
    let seq: u32 = seq_guard.value();

    let entry_guard: redb::AccessGuard<'_, InscriptionEntryValue> = match seq_to_entry.get(&seq)? {
      Some(v) => v,
      None => continue,
    };

    let (_charms, _fee, height, idv, _inscription_number, _parents, sat_opt, _sequence_number, timestamp) =
      entry_guard.value();

    scanned += 1;
    if scanned % 200_000 == 0 {
      eprintln!("progress: scanned={scanned} next_n={n} last_seq={seq}");
    }

    let Some(sat) = sat_opt else { continue };
    if sat < trump_sat_start || sat >= trump_sat_end_exclusive {
      continue;
    }

    hits += 1;
    let id = inscription_id_string(idv);

    let hit = json!({
      "category": "trump_casascius",
      "hit_index": hits,
      "inscription_number": n,
      "sequence_number": seq,
      "inscription_id": id,
      "sat": sat,
      "height": height,
      "timestamp": timestamp,
    });

    eprintln!("HIT #{hits}: inscription_number={n} sat={sat} inscription_id={id}");
    append_jsonl(&hits_path, &hit)?;

    if !first_written {
      if let Some(parent) = Path::new(&first_path).parent() {
        std::fs::create_dir_all(parent)?;
      }
      std::fs::write(&first_path, format!("{hit}\n"))?;
      first_written = true;
    }
  }

  eprintln!("done: scanned={scanned} hits={hits}");
  Ok(())
}
