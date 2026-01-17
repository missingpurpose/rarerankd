use anyhow::{anyhow, bail, Context, Result};
use redb::{Database, ReadableDatabase, TableDefinition, TableHandle};
use serde::Serialize;
use serde_json::json;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

const INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER: TableDefinition<i32, u32> =
  TableDefinition::new("INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER");

type InscriptionIdValue = (u128, u128, u32);
type OutPointValue = (u128, u128, u32); // txid (32 bytes) + vout
type SatPointValue = (OutPointValue, u64); // outpoint + offset
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

const TRUMP_START: u64 = 215_041_381_749_581;
const TRUMP_END_EXCLUSIVE: u64 = 215_041_481_749_581;

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

fn txid_string((head, tail, _vout): OutPointValue) -> String {
  let head = head.to_le_bytes();
  let tail = tail.to_le_bytes();
  let mut bytes = [0u8; 32];
  bytes[..16].copy_from_slice(&head);
  bytes[16..].copy_from_slice(&tail);
  bytes.reverse();
  hex32(bytes)
}

fn satpoint_string((outpoint, offset): SatPointValue) -> String {
  let (_h, _t, vout) = outpoint;
  format!("{}:{}:{}", txid_string(outpoint), vout, offset)
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

fn getenv_i32(name: &str, default: i32) -> i32 {
  env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn arg_value(args: &[String], key: &str) -> Option<String> {
  args.iter()
    .position(|a| a == key)
    .and_then(|i| args.get(i + 1))
    .cloned()
}

fn has_flag(args: &[String], key: &str) -> bool {
  args.iter().any(|a| a == key)
}

fn parse_u64(s: &str, what: &str) -> Result<u64> {
  s.parse::<u64>().with_context(|| format!("invalid {what}: {s}"))
}

fn parse_i32(s: &str, what: &str) -> Result<i32> {
  s.parse::<i32>().with_context(|| format!("invalid {what}: {s}"))
}

#[derive(Serialize)]
struct FirstHitRecord {
  hit_index: u64,
  inscription_number: u64,
  inscription_id: String,
  sat: u64,
  satpoint: String,
  height: u32,
  timestamp: u32,
  category: String,
  // optional extra fields (kept deterministic by struct ordering)
  sequence_number: u32,
}

fn write_single_jsonl_truncate(path: &str, value: &impl Serialize) -> Result<()> {
  if let Some(parent) = Path::new(path).parent() {
    std::fs::create_dir_all(parent).with_context(|| format!("create_dir_all {parent:?}"))?;
  }
  let mut f = OpenOptions::new()
    .create(true)
    .write(true)
    .truncate(true)
    .open(path)
    .with_context(|| format!("open {path} for write"))?;
  serde_json::to_writer(&mut f, value)?;
  f.write_all(b"\n")?;
  Ok(())
}

fn list_satpoint_table_names(db: &Database) -> Result<Vec<String>> {
  let rtx = db.begin_read()?;
  let mut candidates: Vec<String> = vec![];
  for t in rtx.list_tables()? {
    let name = t.name().to_string();
    let upper = name.to_ascii_uppercase();
    if upper.contains("SATPOINT") {
      candidates.push(name);
    }
  }
  candidates.sort();
  Ok(candidates)
}

fn lookup_satpoint(db: &Database, table_name: &str, seq: u32) -> Result<Option<String>> {
  // We intentionally keep this conservative: if the table exists but the type does not match
  // our expected SatPointValue encoding, `open_table` will error and we bubble up.
  let rtx = db.begin_read()?;
  let def: TableDefinition<u32, SatPointValue> = TableDefinition::new(table_name);
  let table = rtx.open_table(def)?;
  let Some(v) = table.get(&seq)? else { return Ok(None); };
  Ok(Some(satpoint_string(v.value())))
}

fn main() -> Result<()> {
  let args: Vec<String> = env::args().collect();

  if has_flag(&args, "--help") || has_flag(&args, "-h") {
    eprintln!(
      "\
trumpfind_redb: scan ord index.redb for Trump Casascius sats hits

USAGE:
  trumpfind_redb [--db-path PATH] [--start N] [--hits-path PATH] [--first-path PATH]
               [--trump-sat-start U64] [--trump-sat-end-exclusive U64]
               [--first-only | --max-hits N] [--first-hits-path PATH]

NOTES:
  - Default full scan behavior is unchanged (appends all hits to hits-path).
  - First-only mode writes exactly one JSONL record (hit_index=1) to first-hits-path and exits.
"
    );
    return Ok(());
  }

  let first_only = has_flag(&args, "--first-only");
  let max_hits = match arg_value(&args, "--max-hits") {
    Some(v) => Some(parse_u64(&v, "--max-hits")?),
    None => None,
  };
  let max_hits = if first_only { Some(1) } else { max_hits };

  let db_path = arg_value(&args, "--db-path")
    .or_else(|| env::var("TRUMPFIND_DB_PATH").ok())
    .unwrap_or_else(|| "/var/lib/ord/index.redb".into());
  let start_n = match arg_value(&args, "--start") {
    Some(v) => parse_i32(&v, "--start")?,
    None => getenv_i32("TRUMPFIND_START", 0),
  };

  let trump_sat_start = match arg_value(&args, "--trump-sat-start") {
    Some(v) => parse_u64(&v, "--trump-sat-start")?,
    None => env::var("TRUMPFIND_TRUMP_SAT_START").ok().map(|v| parse_u64(&v, "TRUMPFIND_TRUMP_SAT_START")).transpose()?.unwrap_or(TRUMP_START),
  };
  let trump_sat_end_exclusive = match arg_value(&args, "--trump-sat-end-exclusive") {
    Some(v) => parse_u64(&v, "--trump-sat-end-exclusive")?,
    None => env::var("TRUMPFIND_TRUMP_SAT_END_EXCLUSIVE").ok().map(|v| parse_u64(&v, "TRUMPFIND_TRUMP_SAT_END_EXCLUSIVE")).transpose()?.unwrap_or(TRUMP_END_EXCLUSIVE),
  };
  if trump_sat_end_exclusive <= trump_sat_start {
    return Err(anyhow!(
      "bad Trump sat range: start={} end_exclusive={}",
      trump_sat_start,
      trump_sat_end_exclusive
    ));
  }

  let hits_path = arg_value(&args, "--hits-path")
    .or_else(|| env::var("TRUMPFIND_HITS_PATH").ok())
    .unwrap_or_else(|| "/var/lib/trumpfind/trump_hits.jsonl".into());
  let first_path = arg_value(&args, "--first-path")
    .or_else(|| env::var("TRUMPFIND_FIRST_PATH").ok())
    .unwrap_or_else(|| "/var/lib/trumpfind/trump_first.json".into());
  let first_hits_path = arg_value(&args, "--first-hits-path")
    .or_else(|| env::var("TRUMPFIND_FIRST_HITS_PATH").ok())
    .unwrap_or_else(|| "trump_hits_first.jsonl".into());

  eprintln!("trumpfind_redb: db_path={db_path}");
  eprintln!("trumpfind_redb: start inscription_number={start_n}");
  eprintln!("trumpfind_redb: trump sat range [{trump_sat_start}, {trump_sat_end_exclusive})");
  if let Some(mh) = max_hits {
    eprintln!("trumpfind_redb: max_hits={mh}");
  }
  if first_only {
    eprintln!("trumpfind_redb: first_only=true first_hits_path={first_hits_path}");
  }

  let db = Database::open(&db_path).with_context(|| format!("open redb {db_path}"))?;
  let rtx = db.begin_read()?;

  let n_to_seq = rtx.open_table(INSCRIPTION_NUMBER_TO_SEQUENCE_NUMBER)?;
  let seq_to_entry = rtx.open_table(SEQUENCE_NUMBER_TO_INSCRIPTION_ENTRY)?;

  let mut scanned: u64 = 0;
  let mut hits: u64 = 0;
  let mut first_written = Path::new(&first_path).exists();
  let satpoint_table_names = list_satpoint_table_names(&db)?;
  if first_only {
    eprintln!("trumpfind_redb: satpoint_table_candidates={:?}", satpoint_table_names);
  }

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

    // Default/full-scan hit record (existing behavior)
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
    if !first_only {
      append_jsonl(&hits_path, &hit)?;
    }

    if !first_written {
      if let Some(parent) = Path::new(&first_path).parent() {
        std::fs::create_dir_all(parent)?;
      }
      std::fs::write(&first_path, format!("{hit}\n"))?;
      first_written = true;
    }

    // First-only / max-hits mode: write only the first hit record to a dedicated JSONL file and exit.
    if hits == 1 {
      if let Some(mh) = max_hits {
        if mh >= 1 {
          if satpoint_table_names.is_empty() {
            bail!("unable to find any SATPOINT table in index.redb; cannot emit required satpoint field");
          }

          let mut satpoint: Option<String> = None;
          for table_name in &satpoint_table_names {
            match lookup_satpoint(&db, table_name, seq) {
              Ok(Some(sp)) => {
                satpoint = Some(sp);
                break;
              }
              Ok(None) => continue,
              Err(e) => {
                // Best-effort probing: ord schema can vary by version; keep scanning candidates.
                eprintln!("WARN: satpoint probe failed table={table_name} seq={seq}: {e:#}");
                continue;
              }
            }
          }
          let satpoint = satpoint
            .ok_or_else(|| anyhow!("satpoint lookup failed for sequence_number={seq} (tried {:?})", satpoint_table_names))?;
          let rec = FirstHitRecord {
            hit_index: 1,
            inscription_number: n as u64,
            inscription_id: id.clone(),
            sat,
            satpoint,
            height,
            timestamp,
            category: "trump_casascius".to_string(),
            sequence_number: seq,
          };
          write_single_jsonl_truncate(&first_hits_path, &rec)?;
          eprintln!("wrote first hit to {first_hits_path}");
          if first_only || mh == 1 {
            eprintln!("exiting after first hit due to first-only/max-hits");
            return Ok(());
          }
        }
      }
    }

    if let Some(mh) = max_hits {
      if hits >= mh && mh > 0 {
        eprintln!("exiting after max-hits={mh}");
        return Ok(());
      }
    }
  }

  eprintln!("done: scanned={scanned} hits={hits}");
  Ok(())
}
