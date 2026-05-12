# AGENTS

## Scope

These instructions apply to the whole repository.

## Project Overview

- Main entrypoint: `ingester.py`
- Database schema: `meshcore_status.sql`
- Python dependency pinning: `requirements.txt`
- Runtime expects MQTT input and ClickHouse output.

## Development Workflow

- Use the existing virtual environment at `.venv` when running Python commands.
- Keep changes focused; this repository is intentionally small and mostly single-file logic.
- Prefer updating `meshcore_status.sql` whenever ingestion changes require new ClickHouse columns.
- Do not commit `queue.db` or other generated runtime artifacts.

## Packet Handling Notes

- Packet decoding is delegated to the external `meshcoredecoder` package pinned in `requirements.txt`.
- When MeshCore packet-format changes affect path decoding, verify both decoder support and local persistence fields in `ingester.py`.
- Treat `pathLength` from decoded packets as hop count, not the raw encoded `path_length` byte.
- Preserve `pathHashSize` and `pathByteLength` when available so newer path-length encodings remain queryable downstream.

## Validation

- For code changes, prefer a focused Python syntax check or a small decode/insert smoke test over broad exploration.
- If you touch schema-related code, validate that inserted column lists still match the table definitions.