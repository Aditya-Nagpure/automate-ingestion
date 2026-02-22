"""
ServiceNow → PostgreSQL Ingestion Pipeline
==========================================
Supports:
  - First load via CSV
  - Delta load via ServiceNow API (plug in credentials when ready)
  - Schema auto-evolution (new columns appear automatically)
  - Watermark-based delta sync (only changed records each run)
  - Sync audit log (every run recorded in sync_log table)
  - Bulk upsert via COPY + staging (fast, safe, idempotent)

Usage:
  First load from CSV:
    python ingest.py --mode first_load --source csv --table incidents

  Delta from CSV (for testing):
    python ingest.py --mode delta --source csv --table incidents

  Delta from API (when credentials are ready):
    python ingest.py --mode delta --source api --table incidents
"""

import csv
import io
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# ---------------------------------------------------
# Bootstrap
# ---------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------
# Config
# ---------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in .env")

# ServiceNow API credentials — leave blank until you have them
SN_INSTANCE = os.getenv("SN_INSTANCE", "")   # e.g. yourcompany.service-now.com
SN_USER     = os.getenv("SN_USER", "")
SN_PASSWORD = os.getenv("SN_PASSWORD", "")

# Table registry
# Add problems / changes / task_sla / user_survey here when ready
TABLE_CONFIG = {
    "incidents": {
        "csv_path":      os.getenv("INCIDENTS_CSV", "data/incident_snow.csv"),
        "api_endpoint":  "/api/now/table/incident",
        "primary_key":   "number",
        "watermark_col": "sys_updated_on",
    },
    # "problems": {
    #     "csv_path":      "data/problem_snow.csv",
    #     "api_endpoint":  "/api/now/table/problem",
    #     "primary_key":   "number",
    #     "watermark_col": "sys_updated_on",
    # },
}

DATETIME_COLUMNS = {
    "opened_at", "resolved_at", "closed_at", "sys_created_on",
    "promoted_on", "proposed_on", "reopened_time", "sys_updated_on",
}

SYNC_LOG_TABLE = "sync_log"

engine = create_engine(DATABASE_URL)


# ===================================================
# SOURCES
# To add ServiceNow API: fill in source_api() below.
# The pipeline beneath doesn't change at all.
# ===================================================

def source_csv(table_name: str, since: datetime | None = None) -> pd.DataFrame:
    """Load from local CSV. `since` simulates delta filtering for testing."""
    cfg = TABLE_CONFIG[table_name]
    path = cfg["csv_path"]
    wm_col = cfg["watermark_col"]

    log.info("Reading CSV: %s", path)
    df = pd.read_csv(
        path,
        encoding="latin1",
        low_memory=False,
        dtype={cfg["primary_key"]: str},
    )
    log.info("Loaded %d rows, %d columns.", len(df), len(df.columns))

    # Apply delta filter if watermark provided
    if since and wm_col in df.columns:
        df[wm_col] = pd.to_datetime(df[wm_col], errors="coerce")
        before = len(df)
        df = df[df[wm_col] > pd.Timestamp(since)]
        log.info("Delta filter (since %s): %d → %d rows.", since.isoformat(), before, len(df))

    return df


def source_api(table_name: str, since: datetime | None = None) -> pd.DataFrame:
    """
    Load from ServiceNow REST API.
    Set SN_INSTANCE, SN_USER, SN_PASSWORD in .env when credentials are ready.

    ServiceNow caps responses at 10,000 records per request.
    Delta runs stay well within this (~200-500 records per 2-3 hour window).
    First load paginates automatically through all records.
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed. Run: pip install requests")

    if not all([SN_INSTANCE, SN_USER, SN_PASSWORD]):
        raise RuntimeError(
            "ServiceNow credentials not configured. "
            "Add SN_INSTANCE, SN_USER, SN_PASSWORD to .env"
        )

    cfg = TABLE_CONFIG[table_name]
    endpoint  = f"https://{SN_INSTANCE}{cfg['api_endpoint']}"
    wm_col    = cfg["watermark_col"]
    page_size = 1000
    all_records = []
    offset = 0

    # Delta: only records updated after watermark
    sysparm_query = ""
    if since:
        ts = since.strftime("%Y-%m-%d %H:%M:%S")
        sysparm_query = f"{wm_col}>{ts}"
        log.info("API delta query: %s > %s", wm_col, ts)
    else:
        log.info("API first load — paginating all records...")

    while True:
        params = {
            "sysparm_limit":  page_size,
            "sysparm_offset": offset,
            "sysparm_query":  sysparm_query,
        }
        resp = requests.get(
            endpoint,
            auth=(SN_USER, SN_PASSWORD),
            params=params,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        records = resp.json().get("result", [])

        if not records:
            break

        all_records.extend(records)
        log.info("  Fetched %d records (offset %d)...", len(records), offset)

        if len(records) < page_size:
            break  # last page
        offset += page_size

    if not all_records:
        log.info("No new records from API.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    log.info("API load complete: %d records.", len(df))
    return df


# ===================================================
# CLEANING
# ===================================================

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase snake_case. Deduplicate if needed."""
    cleaned = []
    seen = {}
    for col in df.columns:
        c = col.strip().lower()
        for ch in (" ", ".", "-"):
            c = c.replace(ch, "_")
        while "__" in c:
            c = c.replace("__", "_")
        c = c.strip("_")

        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cleaned.append(c)

    df.columns = cleaned
    return df


def fix_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and stringify datetime columns so COPY handles them cleanly."""
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # Stringify to ISO format. NaT becomes None -> "" -> NULL via FORCE_NULL.
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S").where(
                df[col].notna(), other=None
            )
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all null representations to Python None."""
    df = df.replace({"NaN": None, "nan": None, "NULL": None, "null": None, "NaT": None})
    df = df.where(df.notna(), other=None)
    return df


def validate_primary_key(df: pd.DataFrame, primary_key: str) -> pd.DataFrame:
    if primary_key not in df.columns:
        raise ValueError(f"Primary key '{primary_key}' not found in data.")

    null_count = df[primary_key].isna().sum()
    if null_count:
        log.warning("Dropping %d row(s) with null primary key.", null_count)
        df = df[df[primary_key].notna()]

    dupes = df[primary_key].duplicated().sum()
    if dupes:
        log.warning("%d duplicate primary key(s) — keeping last occurrence.", dupes)
        df = df.drop_duplicates(subset=[primary_key], keep="last")

    return df


# ===================================================
# SCHEMA
# ===================================================

def q(name: str) -> str:
    """Double-quote an identifier to prevent SQL injection and reserved word conflicts."""
    return f'"{name}"'


def map_dtype(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"


def ensure_table_exists(df: pd.DataFrame, table_name: str, primary_key: str) -> None:
    insp = inspect(engine)
    if insp.has_table(table_name):
        log.info("Table '%s' exists.", table_name)
        return

    log.info("Creating table '%s'...", table_name)
    cols_sql = []
    for col in df.columns:
        col_type = map_dtype(df[col].dtype)
        if col == primary_key:
            cols_sql.append(f"{q(col)} TEXT PRIMARY KEY")
        else:
            cols_sql.append(f"{q(col)} {col_type}")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {q(table_name)} ({', '.join(cols_sql)});"))
    log.info("Table created.")


def add_missing_columns(df: pd.DataFrame, table_name: str) -> None:
    """
    Auto-add new columns that exist in source but not in DB.
    This is what makes new ServiceNow fields appear automatically —
    no manual schema migrations ever needed.
    """
    insp = inspect(engine)
    existing = {col["name"] for col in insp.get_columns(table_name)}
    new_cols = [col for col in df.columns if col not in existing]

    if not new_cols:
        log.info("Schema up to date.")
        return

    log.info("New columns detected — adding: %s", new_cols)
    with engine.begin() as conn:
        for col in new_cols:
            col_type = map_dtype(df[col].dtype)
            conn.execute(text(
                f"ALTER TABLE {q(table_name)} ADD COLUMN {q(col)} {col_type};"
            ))
    log.info("%d column(s) added.", len(new_cols))


# ===================================================
# SYNC LOG
# ===================================================

def ensure_sync_log() -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {q(SYNC_LOG_TABLE)} (
                id              SERIAL PRIMARY KEY,
                table_name      TEXT        NOT NULL,
                run_mode        TEXT        NOT NULL,   -- first_load | delta
                source          TEXT        NOT NULL,   -- csv | api
                started_at      TIMESTAMP   NOT NULL,
                completed_at    TIMESTAMP,
                rows_processed  INTEGER,
                rows_upserted   INTEGER,
                watermark_from  TIMESTAMP,              -- where delta started
                watermark_to    TIMESTAMP,              -- new high watermark
                status          TEXT        NOT NULL,   -- success | failed
                error_message   TEXT
            );
        """))


def log_sync_run(
    table_name: str,
    run_mode: str,
    source: str,
    started_at: datetime,
    rows_processed: int,
    rows_upserted: int,
    watermark_from: datetime | None,
    watermark_to: datetime | None,
    status: str,
    error_message: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {q(SYNC_LOG_TABLE)}
                (table_name, run_mode, source, started_at, completed_at,
                 rows_processed, rows_upserted, watermark_from, watermark_to,
                 status, error_message)
            VALUES
                (:table_name, :run_mode, :source, :started_at, :completed_at,
                 :rows_processed, :rows_upserted, :watermark_from, :watermark_to,
                 :status, :error_message);
        """), {
            "table_name":     table_name,
            "run_mode":       run_mode,
            "source":         source,
            "started_at":     started_at,
            "completed_at":   datetime.now(timezone.utc),
            "rows_processed": rows_processed,
            "rows_upserted":  rows_upserted,
            "watermark_from": watermark_from,
            "watermark_to":   watermark_to,
            "status":         status,
            "error_message":  error_message,
        })
    log.info("Sync run logged to '%s'.", SYNC_LOG_TABLE)


# ===================================================
# WATERMARK
# ===================================================

def get_watermark(table_name: str, watermark_col: str) -> datetime | None:
    """
    Get the highest sys_updated_on from the DB.
    Next delta run will only fetch records newer than this.
    """
    insp = inspect(engine)
    if not insp.has_table(table_name):
        return None

    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT MAX({q(watermark_col)}) FROM {q(table_name)};"
        ))
        val = result.scalar()
        if val is None:
            return None
        wm = pd.Timestamp(val).to_pydatetime()
        log.info("Watermark for '%s': %s", table_name, wm.isoformat())
        return wm


# ===================================================
# BULK UPSERT
# ===================================================

UPSERT_CHUNK_SIZE = int(os.getenv("UPSERT_CHUNK_SIZE", "5000"))


def _upsert_chunk(cur, chunk_df: pd.DataFrame, staging: str, table_name: str,
                  primary_key: str, col_names: str, merge_sql: str,
                  force_null_cols: str) -> int:
    """COPY one chunk into staging, merge to target, drop staging. Returns rowcount."""
    buffer = io.StringIO()
    chunk_df.to_csv(buffer, index=False, na_rep="", quoting=csv.QUOTE_NONNUMERIC)
    buffer.seek(0)

    cur.execute(f"DROP TABLE IF EXISTS {q(staging)};")
    cur.execute(f"CREATE TABLE {q(staging)} (LIKE {q(table_name)});")
    cur.copy_expert(
        f"COPY {q(staging)} ({col_names}) FROM STDIN "
        f"WITH (FORMAT CSV, HEADER TRUE, FORCE_NULL ({force_null_cols}))",
        buffer,
    )
    cur.execute(merge_sql)
    affected = cur.rowcount
    cur.execute(f"DROP TABLE IF EXISTS {q(staging)};")
    return affected


def bulk_upsert(df: pd.DataFrame, table_name: str, primary_key: str) -> int:
    """
    Chunked COPY + merge upsert.

    Each chunk:
      1. COPYs ~5k rows into a small staging table
      2. Merges into target with INSERT ... ON CONFLICT
      3. Drops the staging table immediately

    This keeps disk usage to one chunk at a time (~30MB peak),
    which fits within Render free tier's 1GB disk limit even for 91k rows.
    """
    staging = f"_staging_{table_name}"
    cols = list(df.columns)
    col_names = ", ".join(q(c) for c in cols)
    update_cols = [c for c in cols if c != primary_key]
    update_clause = ", ".join(f"{q(c)}=EXCLUDED.{q(c)}" for c in update_cols)
    force_null_cols = col_names

    merge_sql = f"""
        INSERT INTO {q(table_name)} ({col_names})
        SELECT {col_names} FROM {q(staging)}
        ON CONFLICT ({q(primary_key)})
        DO UPDATE SET {update_clause};
    """

    total = len(df)
    total_affected = 0
    num_chunks = (total + UPSERT_CHUNK_SIZE - 1) // UPSERT_CHUNK_SIZE
    log.info("Upserting %d rows in %d chunk(s) of %d...", total, num_chunks, UPSERT_CHUNK_SIZE)

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            for i in range(0, total, UPSERT_CHUNK_SIZE):
                chunk = df.iloc[i : i + UPSERT_CHUNK_SIZE]
                chunk_num = (i // UPSERT_CHUNK_SIZE) + 1
                log.info("  Chunk %d/%d (%d rows)...", chunk_num, num_chunks, len(chunk))
                affected = _upsert_chunk(
                    cur, chunk, staging, table_name,
                    primary_key, col_names, merge_sql, force_null_cols,
                )
                total_affected += affected
                raw_conn.commit()  # commit each chunk — limits rollback scope

        log.info("All chunks complete: %d rows affected.", total_affected)
        return total_affected

    except Exception as e:
        raw_conn.rollback()
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {q(staging)};"))
        except Exception:
            pass
        raise e
    finally:
        raw_conn.close()


# ===================================================
# CORE PIPELINE
# ===================================================

def run_pipeline(table_name: str, run_mode: str, source: str) -> None:
    """
    Orchestrates a full pipeline run:
      1. Get watermark (delta only)
      2. Fetch data from source
      3. Clean and validate
      4. Evolve schema
      5. Bulk upsert
      6. Log run to sync_log
    """
    cfg = TABLE_CONFIG[table_name]
    primary_key   = cfg["primary_key"]
    watermark_col = cfg["watermark_col"]

    started_at     = datetime.now(timezone.utc)
    watermark_from = None
    watermark_to   = None
    rows_processed = 0
    rows_upserted  = 0

    ensure_sync_log()

    try:
        # 1. Watermark
        if run_mode == "delta":
            watermark_from = get_watermark(table_name, watermark_col)
            if watermark_from is None:
                log.warning(
                    "No watermark found — table is empty or doesn't exist yet. "
                    "Run with --mode first_load first."
                )

        since = watermark_from if run_mode == "delta" else None

        # 2. Fetch
        if source == "csv":
            df = source_csv(table_name, since=since)
        elif source == "api":
            df = source_api(table_name, since=since)
        else:
            raise ValueError(f"Unknown source '{source}'. Use 'csv' or 'api'.")

        if df.empty:
            log.info("No new data. Nothing to upsert.")
            log_sync_run(
                table_name, run_mode, source, started_at,
                0, 0, watermark_from, watermark_from, "success"
            )
            return

        rows_processed = len(df)

        # 3. Clean
        log.info("Cleaning...")
        df = clean_columns(df)
        df = fix_datetime_columns(df)
        df = clean_dataframe(df)
        df = validate_primary_key(df, primary_key)

        # Capture new watermark from this batch
        if watermark_col in df.columns:
            wm_series = pd.to_datetime(df[watermark_col], errors="coerce")
            wm_max = wm_series.max()
            if pd.notna(wm_max):
                watermark_to = wm_max.to_pydatetime()

        # 4. Schema
        log.info("Checking schema...")
        ensure_table_exists(df, table_name, primary_key)
        add_missing_columns(df, table_name)

        # 5. Upsert
        rows_upserted = bulk_upsert(df, table_name, primary_key)

        # 6. Log
        log_sync_run(
            table_name, run_mode, source, started_at,
            rows_processed, rows_upserted,
            watermark_from, watermark_to,
            "success"
        )

        log.info(
            "=== Done: %d processed, %d upserted. Watermark advanced to: %s ===",
            rows_processed, rows_upserted, watermark_to,
        )

    except Exception as e:
        log.error("Pipeline failed: %s", e)
        log_sync_run(
            table_name, run_mode, source, started_at,
            rows_processed, rows_upserted,
            watermark_from, watermark_to,
            "failed", str(e)
        )
        raise


# ===================================================
# CLI
# ===================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ServiceNow → PostgreSQL ingestion pipeline"
    )
    parser.add_argument(
        "--table",
        default="incidents",
        choices=list(TABLE_CONFIG.keys()),
        help="Table to sync (default: incidents)",
    )
    parser.add_argument(
        "--mode",
        default="delta",
        choices=["first_load", "delta"],
        help="first_load = full reload | delta = changed records only (default: delta)",
    )
    parser.add_argument(
        "--source",
        default="csv",
        choices=["csv", "api"],
        help="Data source (default: csv)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    log.info("=== ServiceNow Ingestion Pipeline ===")
    log.info("Table:  %s", args.table)
    log.info("Mode:   %s", args.mode)
    log.info("Source: %s", args.source)

    run_pipeline(
        table_name=args.table,
        run_mode=args.mode,
        source=args.source,
    )