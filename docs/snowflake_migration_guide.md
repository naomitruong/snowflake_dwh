# Snowflake Migration Guide — vn_ministop_dwh

Step-by-step guide to rebuild the `vn_ministop_dwh` pipeline on Snowflake only (no GCP or AWS
account needed). Daily CSV files are kept in a local folder, uploaded to a Snowflake internal
stage, loaded into raw tables, then transformed by dbt:

```
gcs_download/<year>/<YYYYMMDD>/<name>_<YYYYMMDD>.csv          (local folder)
        │  PUT (ingest/ingest.py)
        ▼
@MSV_DWH_DEV.VN_MMS_RAW_STOCK.MMS_STAGE/<year>/<YYYYMMDD>/   (internal stage, .csv.gz)
        │  COPY INTO (procedure LOAD_RAW_STOCK)
        ▼
VN_MMS_RAW_STOCK.<NAME>     raw tables, all columns VARCHAR + EXECUTE_TARGET_DATE
        │  dbt (incremental tables)
        ▼
VN_MMS_ALL.*                typed, cleaned tables
        │  dbt (views)
        ▼
VN_MMS.*                    views for BI / Supabase seed
```

Requirements kept from the BigQuery setup: idempotent reloads per delivery date, dbt
transform + views, tests, freshness checks, and the Supabase seed jobs.

---

## 0. Mapping: current setup → Snowflake

| Current (BigQuery / GCP) | Snowflake |
|---|---|
| GCP projects `newbi-msv-cloud-dwh-dev` / `newbi-msv-cloud-dwh` / `ff-project-511710` | One account; one database per environment: `MSV_DWH_DEV`, `MSV_DWH_PROD` |
| Datasets `vn_ministop_raw_stock`, `vn_ministop_all`, `vn_ministop` | Schemas `VN_MMS_RAW_STOCK`, `VN_MMS_ALL`, `VN_MMS` |
| Source CSVs on a GCS bucket | Local folder `gcs_download/` → internal stage `VN_MMS_RAW_STOCK.MMS_STAGE` |
| Cloud Run job `ingest/load_raw.py` (header check, staging, DELETE+INSERT per `execute_target_date`) | `ingest/ingest.py` (`PUT` to stage) + stored procedure `LOAD_RAW_STOCK` (DELETE + `COPY INTO` per date) |
| Typed raw columns (`config.py` casts) | Raw columns all `VARCHAR`; casting moves to the `VN_MMS_ALL` dbt models |
| `PARTITION BY execute_target_date`, `CLUSTER BY receipt_date, store_id` | No partitions; use `CLUSTER BY` (micro-partitions) |
| dbt-bigquery, `insert_overwrite` + `partition_by` | dbt-snowflake, `delete+insert` + `unique_key` |
| `maximum_bytes_billed`, `job_execution_timeout_seconds` | Resource Monitor + `STATEMENT_TIMEOUT_IN_SECONDS` |
| `+labels` | `+query_tag` |
| Cloud Workflows + Cloud Scheduler | Snowflake Tasks (raw load → dbt) |
| `periodic_seed` reads BigQuery → Supabase | Read from `VN_MMS` on Snowflake |

---

## Step 1 — Snowflake account

1. Sign up at <https://signup.snowflake.com>. **Standard** edition is enough unless you need
   multi-cluster warehouses or time travel > 1 day (**Enterprise**).
2. Pick a region close to Vietnam (e.g. AWS `ap-southeast-1` Singapore). The cloud provider
   behind the account does not require you to have an account with that provider.
3. Get the account identifier, used by every client below:
   ```sql
   SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();   -- e.g. GOYCSTU-VG36275
   ```

---

## Step 2 — Databases, schemas, warehouses, roles

Run in Snowsight as `ACCOUNTADMIN`:

```sql
-- Warehouses: XS, auto-suspend after 60s
CREATE WAREHOUSE WH_INGEST WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE WAREHOUSE WH_DBT WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 1800;          -- = job_execution_timeout_seconds

-- Cost guard (replaces maximum_bytes_billed): monthly credit quota
CREATE RESOURCE MONITOR RM_DWH WITH CREDIT_QUOTA = 50 FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE WH_DBT    SET RESOURCE_MONITOR = RM_DWH;
ALTER WAREHOUSE WH_INGEST SET RESOURCE_MONITOR = RM_DWH;

-- One database per environment (repeat for MSV_DWH_PROD)
CREATE DATABASE MSV_DWH_DEV;
CREATE SCHEMA MSV_DWH_DEV.VN_MMS_RAW_STOCK;   -- stage + raw tables
CREATE SCHEMA MSV_DWH_DEV.VN_MMS_ALL;         -- dbt tables
CREATE SCHEMA MSV_DWH_DEV.VN_MMS;             -- dbt views

-- Roles (equivalent of sa-dbt-runner, and read-only consumers)
CREATE ROLE DWH_TRANSFORMER;   -- upload + raw load + dbt
CREATE ROLE DWH_READER;        -- BI / Supabase seed, read only

GRANT USAGE ON WAREHOUSE WH_INGEST TO ROLE DWH_TRANSFORMER;
GRANT USAGE ON WAREHOUSE WH_DBT    TO ROLE DWH_TRANSFORMER;
GRANT USAGE, CREATE SCHEMA ON DATABASE MSV_DWH_DEV TO ROLE DWH_TRANSFORMER;
GRANT ALL ON ALL SCHEMAS IN DATABASE MSV_DWH_DEV   TO ROLE DWH_TRANSFORMER;   -- run after the schemas exist
GRANT ALL ON FUTURE TABLES IN DATABASE MSV_DWH_DEV TO ROLE DWH_TRANSFORMER;
GRANT ALL ON FUTURE VIEWS  IN DATABASE MSV_DWH_DEV TO ROLE DWH_TRANSFORMER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DWH_TRANSFORMER;                       -- for Step 7

GRANT USAGE ON WAREHOUSE WH_DBT TO ROLE DWH_READER;
GRANT USAGE ON DATABASE MSV_DWH_DEV TO ROLE DWH_READER;
GRANT USAGE ON SCHEMA MSV_DWH_DEV.VN_MMS TO ROLE DWH_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MSV_DWH_DEV.VN_MMS TO ROLE DWH_READER;

GRANT ROLE DWH_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE DWH_READER      TO ROLE SYSADMIN;
```

---

## Step 3 — Service user with key-pair auth (no passwords)

Generate the key pair locally (`openssl`, not `openssle`):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out sf_dbt_runner.p8   # private key, keep secret
openssl rsa -in sf_dbt_runner.p8 -pubout -out sf_dbt_runner.pub             # public key, goes to Snowflake
grep -v "PUBLIC KEY" sf_dbt_runner.pub | tr -d '\n'; echo                    # one-line value for RSA_PUBLIC_KEY
```

Create the user with the **public** key (pasting the private key gives `Invalid Public key`):

```sql
CREATE USER SVC_DBT_RUNNER TYPE = SERVICE
  DEFAULT_ROLE = DWH_TRANSFORMER
  DEFAULT_WAREHOUSE = WH_INGEST
  RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A...';
GRANT ROLE DWH_TRANSFORMER TO USER SVC_DBT_RUNNER;   -- without it: "Role 'DWH_TRANSFORMER' ... is not granted to this user"

DESC USER SVC_DBT_RUNNER;   -- RSA_PUBLIC_KEY_FP must show SHA256:...
```

Keep `sf_dbt_runner.p8` outside git (e.g. `~/.snowflake/sf_dbt_runner.p8`, `chmod 600`).
Create a second user (e.g. `SVC_SEED_READER` with `DWH_READER`) for `periodic_seed` if you
want least privilege.

Local Python setup (Ubuntu blocks system-wide `pip install`, so use a virtualenv):

```bash
python3 -m venv .venv
.venv/bin/pip install "snowflake-connector-python[pandas]"
```

---

## Step 4 — Upload the local folder to the internal stage

Create the file format and stage once (as `DWH_TRANSFORMER`):

```sql
USE ROLE DWH_TRANSFORMER;
USE SCHEMA MSV_DWH_DEV.VN_MMS_RAW_STOCK;

CREATE FILE FORMAT IF NOT EXISTS CSV_RAW
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ENCODING = 'UTF8'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE   -- replaces clean_csv_stream (pad/truncate columns)
  EMPTY_FIELD_AS_NULL = FALSE;

CREATE STAGE IF NOT EXISTS MMS_STAGE
  DIRECTORY = (ENABLE = TRUE)              -- lets you list files with DIRECTORY(@MMS_STAGE)
  FILE_FORMAT = CSV_RAW;
```

Upload with `ingest/ingest.py`, run **from the project root** (`PUT` cannot run in a Snowsight
worksheet). It keeps the `<year>/<YYYYMMDD>/` folder structure; `AUTO_COMPRESS` gzips each file
(`pos_20260817.csv` → `pos_20260817.csv.gz`), and `COPY INTO` decompresses it automatically.

```python
from pathlib import Path
import snowflake.connector

root = Path("gcs_download")
with snowflake.connector.connect(
    account="GOYCSTU-VG36275", user="SVC_DBT_RUNNER",
    private_key_file="sf_dbt_runner.p8", role="DWH_TRANSFORMER",
    warehouse="WH_INGEST", database="MSV_DWH_DEV", schema="VN_MMS_RAW_STOCK",
) as conn, conn.cursor() as cur:
    for d in sorted(p for p in root.glob("*/*") if p.is_dir()):
        sub = d.relative_to(root).as_posix()          # e.g. 2026/20260817
        cur.execute(f"PUT 'file://{d.resolve()}/*.csv' @MMS_STAGE/{sub}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        print(sub, len(cur.fetchall()), "files")
    cur.execute("ALTER STAGE MMS_STAGE REFRESH")
```

```bash
.venv/bin/python ingest/ingest.py
```

Check: `SELECT * FROM DIRECTORY(@MMS_STAGE);` should list 8 files per date
(`category`, `pos`, `product`, `product_sales_report`, `promotion`, `promotion_bundle`,
`store`, `vendor`). For a one-off upload you can also use Snowsight: **Catalog → Database
Explorer → VN_MMS_RAW_STOCK → Stages → MMS_STAGE → + Files**, setting the path to
`<year>/<YYYYMMDD>/`.

---

## Step 5 — Load stage → raw tables (`VN_MMS_RAW_STOCK`)

Setup SQL is in [`sql/raw_stock_pipeline.sql`](../sql/raw_stock_pipeline.sql). Sections 1–4 of that file create:

- **One raw table per file type** (`POS`, `PRODUCT`, `PRODUCT_SALES_REPORT`, `PROMOTION`,
  `PROMOTION_BUNDLE`, `STORE`, `VENDOR`, `CATEGORY`). The CSV columns come first, in file order,
  all `VARCHAR` (header names upper-cased, non-alphanumerics → `_`, e.g. `No.` → `NO`), followed by
  `EXECUTE_TARGET_DATE DATE`, `SOURCE_FILE VARCHAR`, `LOADED_AT TIMESTAMP_LTZ`.
- **`LOAD_LOG`**: one row per file load attempt (`LOADED` / `FAILED` + error).
- **Procedure `LOAD_RAW_STOCK()`**: refreshes the stage directory, finds files not yet loaded
  (or re-uploaded, by `LAST_MODIFIED`), and for each file runs in one transaction:

  ```sql
  DELETE FROM <TABLE> WHERE EXECUTE_TARGET_DATE = '<date>';
  COPY INTO <TABLE> (<csv columns>, EXECUTE_TARGET_DATE, SOURCE_FILE, LOADED_AT)
  FROM (SELECT $1, $2, ..., '<date>'::DATE, METADATA$FILENAME, CURRENT_TIMESTAMP() FROM @MMS_STAGE)
  FILES = ('<year>/<YYYYMMDD>/<name>_<YYYYMMDD>.csv.gz')
  FILE_FORMAT = (FORMAT_NAME = CSV_RAW)
  ON_ERROR = ABORT_STATEMENT
  FORCE = TRUE;            -- reloading the same date must not be skipped by load history
  ```

  Table name and date come from the file name (`pos_20260817.csv.gz` → `POS`, `2026-08-17`), so
  re-uploading a date replaces that date only (same as the BigQuery DELETE+INSERT).

Run a load manually:

```sql
CALL MSV_DWH_DEV.VN_MMS_RAW_STOCK.LOAD_RAW_STOCK();

SELECT * FROM LOAD_LOG ORDER BY LOADED_AT DESC;
SELECT EXECUTE_TARGET_DATE, COUNT(*) FROM POS GROUP BY 1 ORDER BY 1;
```

Notes:
- Columns are matched **by position**. If the source adds, removes or reorders a column, update
  the table DDL first, otherwise the load shifts data into the wrong columns. Check headers
  before uploading a new delivery.
- `pos_remove_list` is not part of the daily files. Create it once, load the current snapshot
  (export from BigQuery to CSV → `PUT` to `@MMS_STAGE/manual/` → `COPY INTO`), and add rows by
  hand afterwards (`scripts/import_wholesale_remove_list.py` must be ported to the Snowflake
  connector).

---

## Step 6 — dbt: `VN_MMS_RAW_STOCK` → `VN_MMS_ALL` → `VN_MMS`

### a. `requirements.txt`
```
dbt-core~=1.12.0
dbt-snowflake~=1.12.0
```

### b. `profiles.yml`
```yaml
vn_ministop:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: snowflake
      account: GOYCSTU-VG36275
      user: SVC_DBT_RUNNER
      authenticator: snowflake_jwt
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: DWH_TRANSFORMER
      warehouse: WH_DBT
      database: MSV_DWH_DEV
      schema: VN_MMS_ALL
      threads: 4
      query_tag: vn_ministop
      client_session_keep_alive: false
    prod:
      type: snowflake
      account: GOYCSTU-VG36275
      user: SVC_DBT_RUNNER
      authenticator: snowflake_jwt
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: DWH_TRANSFORMER
      warehouse: WH_DBT
      database: MSV_DWH_PROD
      schema: VN_MMS_ALL
      threads: 4
      query_tag: vn_ministop
```

### c. Sources and schemas (`dbt_project.yml`, `models/sources/raw_stock.yml`)
- Source `raw_stock` → database `MSV_DWH_DEV`, schema `VN_MMS_RAW_STOCK`; table names are the
  upper-case file names (`POS`, `PRODUCT`, ...). `var('raw_project')` now means the raw **database**.
- Models that were in `vn_ministop_all` → `+schema` resolves to `VN_MMS_ALL` (materialized as
  tables / incremental); models that were in `vn_ministop` → `VN_MMS` (views).
  Keep `generate_schema_name` returning the custom schema as-is so names are not prefixed.
- `+labels: {pipeline: vn_ministop}` → `+query_tag: vn_ministop`.
- Source freshness: use `loaded_at_field: LOADED_AT` on the raw tables.

### d. Casting moves into `VN_MMS_ALL`
Raw columns are all `VARCHAR`, so the first models reading `raw_stock` must cast (this replaces
`config.py` / `select_list()`):

```sql
select
    try_to_date(nullif(trim(sales_date), ''))            as receipt_date,
    try_cast(nullif(trim(sales_qty), '') as number(38,9)) as quantity,
    ...
    execute_target_date
from {{ source('raw_stock', 'POS') }}
```

Use `TRY_*` so bad values become `NULL` instead of failing the build, and add `not_null` /
`accepted_values` tests where a NULL would hide a source problem.

### e. Incremental models — most important change
On Snowflake, `incremental_strategy='insert_overwrite'` **overwrites the whole table**, not
per partition as on BigQuery. Change the models that use it to:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='receipt_date',
    cluster_by=['receipt_date'],
) }}
```

`delete+insert` with `unique_key='receipt_date'` deletes every date present in the new batch
and inserts it again, which is equivalent to BigQuery partition overwrite. Remove
`partition_by`.

### f. SQL translation

Functions found in `dbt/models` and `dbt/tests`:

| BigQuery | Snowflake |
|---|---|
| `PARSE_DATE('%Y%m%d', x)` | `TO_DATE(x, 'YYYYMMDD')` |
| `SAFE.PARSE_DATE('%d/%m/%Y', x)` | `TRY_TO_DATE(x, 'DD/MM/YYYY')` |
| `SAFE_CAST(x AS T)` | `TRY_CAST(x AS T)` (input must be a string) |
| `DATE_SUB(d, INTERVAL n DAY)` | `DATEADD(day, -n, d)` or `{{ dbt.dateadd('day', -n, 'd') }}` |
| `DATE_ADD(d, INTERVAL n DAY)` | `DATEADD(day, n, d)` |
| `INT64` / `INTEGER` | `NUMBER(38,0)` |
| `NUMERIC` | `NUMBER(38,9)` |
| `STRING` | `VARCHAR` |
| `TIMESTAMP(DATETIME(d, PARSE_TIME('%H:%M:%S', t)), 'Asia/Ho_Chi_Minh')` | `CONVERT_TIMEZONE('Asia/Ho_Chi_Minh', 'UTC', TIMESTAMP_NTZ_FROM_PARTS(d, TO_TIME(t, 'HH24:MI:SS')))` |
| `UNNEST(arr)` | `LATERAL FLATTEN(input => arr)` (use `value`) |
| `` `col` `` | `col` or `"col"` |
| `QUALIFY`, `LPAD`, `ARRAY_AGG`, `COALESCE`, `NULLIF` | unchanged |

Also update the `where` conditions of tests in `_*.yml` and `models/sources/raw_stock.yml`:
`date_sub(current_date(), interval N day)` → `dateadd(day, -N, current_date())`. Prefer
`{{ dbt.dateadd(...) }}` so the code is portable across both warehouses.

`macros/event_date.sql` needs no change (it only returns a quoted `'YYYYMMDD'` string).

### g. Local check

```bash
cd dbt
../.venv/bin/pip install -r requirements.txt
export SNOWFLAKE_PRIVATE_KEY_PATH=~/.snowflake/sf_dbt_runner.p8
dbt deps
dbt debug --target dev
dbt seed
dbt source freshness
dbt build --vars '{event_date: "20260817"}'
```

Start with small models (`category`, `shop`) to validate profile and macros before porting `pos`.

---

## Step 7 — Orchestration (Snowflake Tasks)

Daily run, all inside Snowflake:

1. **Upload** (outside Snowflake): the files are local, so `ingest/ingest.py` must run on the
   machine that holds the folder (manually or via `cron`).
2. **Raw load**: task `LOAD_RAW_STOCK_TASK` (section 5 of `sql/raw_stock_pipeline.sql`) calls
   `LOAD_RAW_STOCK()` on a schedule, e.g. `USING CRON 0 7 * * * Asia/Ho_Chi_Minh`. It fails the
   run when any file fails, so errors show in `TASK_HISTORY`; details are in `LOAD_LOG`.
3. **dbt**: either run `dbt build` from the same machine after the load, or deploy the project as
   a dbt Project on Snowflake and chain a task after the raw load:
   ```sql
   CREATE TASK DBT_BUILD_TASK
     WAREHOUSE = WH_DBT
     AFTER LOAD_RAW_STOCK_TASK
     AS EXECUTE DBT PROJECT <db>.<schema>.<dbt_project> ARGS = 'build --target dev';
   ALTER TASK DBT_BUILD_TASK RESUME;     -- resume child tasks before the root task
   ```

Monitoring:

```sql
SELECT * FROM TABLE(MSV_DWH_DEV.INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC;
SELECT * FROM MSV_DWH_DEV.VN_MMS_RAW_STOCK.LOAD_LOG WHERE STATUS = 'FAILED';
```

---

## Step 8 — periodic_seed (Supabase)

`periodic_seed/scripts/promotion_bq_source.py`, `sync_newbi_masters.py` and related scripts
read BigQuery. Switch them to the `VN_MMS` views:

```python
cur = conn.cursor()
cur.execute("SELECT ... FROM VN_MMS.PROMOTION")
df = cur.fetch_pandas_all()
```

Use the read-only role `DWH_READER`, and replace `google-cloud-bigquery[pandas]` with
`snowflake-connector-python[pandas]` in `periodic_seed/requirements.txt`. Snowflake returns
column names in UPPER CASE; lower-case them (`df.columns = df.columns.str.lower()`) if downstream
code expects lower case.

---

## Step 9 — Backfill, reconcile, cut over

1. **Backfill raw**: put every historical date folder under `gcs_download/`, run
   `ingest/ingest.py`, then `CALL LOAD_RAW_STOCK();` (it loads all files not yet in `LOAD_LOG`).
2. **Build**: `dbt build --full-refresh` on Snowflake.
3. **Reconcile** with BigQuery for each view in `vn_ministop` vs `VN_MMS`:
   row counts, `SUM(quantity)`, `SUM(incl_tax_sales_amount)` grouped by `receipt_date` ×
   `shop_id`. `scripts/compare_compiled.py` can serve as a starting point.
4. **Parallel run** for 1–2 weeks, then point BI and Supabase seed jobs to Snowflake and
   pause the BigQuery schedules.

---

## Checklist

- [ ] Account created, account identifier noted (Step 1)
- [ ] Warehouses, resource monitor, database, schemas `VN_MMS_RAW_STOCK` / `VN_MMS_ALL` / `VN_MMS`, roles (Step 2)
- [ ] `SVC_DBT_RUNNER` with key-pair auth, `DWH_TRANSFORMER` granted (Step 3)
- [ ] `MMS_STAGE` created; `DIRECTORY(@MMS_STAGE)` lists the uploaded files (Step 4)
- [ ] Raw tables + `LOAD_RAW_STOCK` created; one date loaded and counts checked (Step 5)
- [ ] `pos_remove_list` loaded (Step 5)
- [ ] dbt profile works, small models built in `VN_MMS_ALL` / `VN_MMS` (Step 6)
- [ ] All models translated; incremental models use `delete+insert` (Step 6e–f)
- [ ] Tasks scheduled for raw load → dbt (Step 7)
- [ ] periodic_seed reads Snowflake (Step 8)
- [ ] History backfilled, numbers reconciled, cut-over done (Step 9)
