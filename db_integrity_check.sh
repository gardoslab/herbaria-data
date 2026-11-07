#!/bin/bash -l

# Run SQLite's PRAGMA integrity_check against download_status.db.
#
# Read-only against the live DB -- in WAL mode the running downloader can
# keep writing while this reads a consistent snapshot. On a ~19 GB DB this
# typically takes ~10-60 minutes; h_rt below is set to 4 h for headroom.
#
# Also prints fresh row counts at the end so we can cross-check against the
# numbers status_report.py was seeing.

module load miniconda
module load academic-ml/spring-2026

conda activate spring-2026-pyt

DB=/projectnb/herbdl/data/GBIF-F25h/download_status.db

echo "=== PRAGMA integrity_check on $DB ==="
echo "started: $(date)"
python3 -u -c "
import sqlite3
conn = sqlite3.connect('file:$DB?mode=ro', uri=True, timeout=300)
conn.execute('PRAGMA busy_timeout=300000')
print('  running PRAGMA integrity_check ...', flush=True)
for row in conn.execute('PRAGMA integrity_check'):
    print(f'    {row[0]}', flush=True)
print()
print('  row counts (live snapshot):', flush=True)
for t in ('images', 'gbif_ids', 'hosts'):
    n = conn.execute('SELECT COUNT(*) FROM ' + t).fetchone()[0]
    print(f'    {t}: {n:,} rows', flush=True)
print('  gbif_ids by status:', flush=True)
for status, n in conn.execute('SELECT status, COUNT(*) FROM gbif_ids GROUP BY status'):
    print(f'    {status}: {n:,}', flush=True)
"
echo "finished: $(date)"

### The command below is used to submit the job to the cluster:
### qsub -N db_integrity -l h_rt=4:00:00 -pe omp 4 -P herbdl -j y -o db_integrity.out db_integrity_check.sh
