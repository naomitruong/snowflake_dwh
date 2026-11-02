cd /mnt/DATA/Projects/Ref-git-repo/snowflake-dwh
.venv/bin/python - <<'EOF'
from pathlib import Path
import snowflake.connector

root = Path("gcs_download")
with snowflake.connector.connect(
    account="GOYCSTU-VG36275", user="SVC_DBT_RUNNER",
    private_key_file="sf_dbt_runner.p8", role="DWH_TRANSFORMER",
    warehouse="WH_INGEST", database="MSV_DWH_DEV", schema="VN_MMS_RAW_STOCK",
) as conn, conn.cursor() as cur:
    for d in sorted(p for p in root.glob("*/*") if p.is_dir()):
        sub = d.relative_to(root).as_posix()          # vd: 2026/20260817
        cur.execute(f"PUT 'file://{d.resolve()}/*.csv' @MMS_STAGE/{sub}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        print(sub, len(cur.fetchall()), "files")
    cur.execute("ALTER STAGE MMS_STAGE REFRESH")
EOF
