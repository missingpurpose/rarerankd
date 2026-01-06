# trumpfind-hits (Trump Casascius dataset exports)

This branch publishes the **Trump Casascius** hit list artifacts derived from a production `ord` database scan.

## What this is

- **Primary artifact**: `trump_hits.jsonl` (one JSON object per hit).
- **Purpose**: provide a stable, verifiable dataset that downstream tooling can download and analyze.
- **Source of truth**: the scan is derived from `ord`’s native database (`index.redb`) on a fully-synced Bitcoin Core node.

## Category definition

Trump sat range (full 1 BTC contiguous window):

- `TRUMP_START = 215041381749581`
- `TRUMP_END_INCLUSIVE = 215041481749580`
- `TRUMP_END_EXCLUSIVE = 215041481749581`

## File formats (expected)

### `trump_hits.jsonl`

JSON Lines. One object per hit. Typical fields:

- `inscription_number`
- `inscription_id`
- `sat`
- `satpoint`
- `height`
- `timestamp`

## Integrity / verification

Consumers should verify integrity using the checksum manifest produced alongside the exports.

The canonical checksum manifest + evidence bundle are referenced from the infra runbooks repo:
- `alkanes-infra-runbooks/status/trump-casascius-rarerank.md`

## Operational note (DB locking)

When generating these exports from production:

- `ord-server` holds an exclusive lock on `index.redb`.
- Stop `ord-server`, run the scan, then start `ord-server` again immediately after.

## Security

Do not commit:
- secrets
- cookie contents
- private keys
- server-specific credentials


