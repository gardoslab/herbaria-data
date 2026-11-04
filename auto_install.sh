#!/bin/bash -l
#$ -N image_install_auto
#$ -l h_rt=48:00:00
#$ -pe omp 16
#$ -P herbdl
#$ -cwd
#$ -m a
#$ -M ljhao@bu.edu

# Self-chaining download job: runs one image_install_db.sh round, but first
# queues the next round behind itself, so the pipeline keeps going without a
# manual qsub every 48 hours.
#
# SGE hard-kills the job at h_rt, so nothing at the end of this script is
# guaranteed to run. All bookkeeping therefore happens at the START of the
# round: report on the previous round, write auto_state.txt, then
# `qsub -hold_jid $JOB_ID` the next round. The hold releases however this job
# ends (finished, crashed, h_rt kill, qdel) and only after it has fully
# exited, so two rounds never have the database open at the same time. As a
# backstop, a round also stops before touching the database if qstat shows
# another image_inst* job (a manual round or a second chain) or a db_integ* job
# (db_integrity_check.sh) running.
#
#   start:                  qsub auto_install.sh
#   behind a running job:   qsub -hold_jid <jobid> auto_install.sh
#   stop:                   touch STOP_AUTO   (honoured at the next round)
#
# auto_state.txt is one line: <total_at_round_start> <start_epoch> <round>.
# total is "NA" if the success count failed at the start of that round.
# See README.md (auto_install.sh) for details.

REPO=/projectnb/herbdl/workspaces/ljhao/herbaria-data
DB=/projectnb/herbdl/data/GBIF-F25h/download_status.db
STATE_FILE=auto_state.txt
STOP_FILE=STOP_AUTO
MIN_ROUND_SECONDS=3600        # a shorter previous round means a crash or an empty queue
TARGET_IMAGES=50000000        # ~50M, for the "% done" figure

# Same query as the existing manual baselines, so the numbers line up.
COUNT_SQL="SELECT SUM(n) FROM (SELECT (SELECT COUNT(*) FROM images i WHERE i.host = h.host AND i.status = 'success') AS n FROM hosts h)"

now=$(date +%s)

cd "$REPO" || { echo "auto_install: cannot cd to $REPO" >&2; exit 1; }

# Outside SGE there is no job to hold the next round on, so it could start
# while this one still has the database open. Refuse to run.
if [[ -z "${JOB_ID:-}" ]]; then
    echo "auto_install: JOB_ID not set -- submit with qsub, don't run directly." >&2
    exit 1
fi

module load miniconda academic-ml/spring-2026
if ! conda activate spring-2026-pyt; then
    echo "auto_install: conda activate spring-2026-pyt failed; nothing run or queued." >&2
    exit 1
fi

# --- helpers ----------------------------------------------------------------

notify() {
    echo "[$(date '+%F %T')] $1"
    python -c 'import sys; from notifications import send_notification; send_notification("Image Installation (auto)", sys.argv[1])' "$1"
}

commas() { sed ':a;s/\B[0-9]\{3\}\>/,&/;ta' <<< "$1"; }
pct()    { awk -v n="$1" -v t="$TARGET_IMAGES" 'BEGIN { printf "%.1f", 100 * n / t }'; }
hours()  { awk -v s="$1" 'BEGIN { printf "%.1f", s / 3600 }'; }

# Prints the number of successful images, or an error message and exits 1.
# Read-only (mode=ro): this job only ever reads the database here, and the
# connection is closed before image_install_db.sh opens it.
count_success() {
    python - "$DB" "$COUNT_SQL" <<'PY'
import sqlite3, sys, urllib.parse
db, sql = sys.argv[1], sys.argv[2]
try:
    con = sqlite3.connect("file:" + urllib.parse.quote(db) + "?mode=ro",
                          uri=True, timeout=300)
    try:
        (n,) = con.execute(sql).fetchone()
    finally:
        con.close()
except Exception as e:
    print(f"{type(e).__name__}: {e}")
    sys.exit(1)
print(n or 0)
PY
}

# --- stop requested ---------------------------------------------------------

if [[ -e $STOP_FILE ]]; then
    notify "$STOP_FILE found -- auto-run stopped, nothing queued (job $JOB_ID). To resume: rm -f $STOP_FILE $STATE_FILE && qsub auto_install.sh"
    exit 0
fi

# --- refuse to run beside another job using the database --------------------
# hold_jid keeps the chain's own rounds apart, but not a manual submission, a
# second chain, or an integrity check. qstat truncates names to 10 characters:
# image_install_db and image_install_auto both show as "image_inst", and
# db_integrity (db_integrity_check.sh) as "db_integri". Running-type states
# count: r; t (being dispatched -- two jobs started together must not both
# pass); Rr/dr; and suspended s/S/T, which still hold the database open.
# Pending states (qw, hqw, Eqw, Rq) contain none of those letters. If qstat
# keeps failing we can't rule out a clash, so stop rather than risk it.

qstat_ok=""
for attempt in 1 2 3; do
    if qstat_out=$(qstat -u ljhao 2>&1); then
        qstat_ok=1
        break
    fi
    (( attempt < 3 )) && sleep 60
done
if [[ -z $qstat_ok ]]; then
    notify "STOPPED: qstat failed 3 times (${qstat_out//$'\n'/ }), so another job using the database can't be ruled out. Job $JOB_ID did nothing (no count, nothing queued, no download). Resubmit with: qsub auto_install.sh"
    exit 2
fi

running=$(awk -v me="$JOB_ID" '
    $3 ~ /^(image_inst|db_integ)/ && $5 ~ /[rtsST]/ && $1 != me { print $1, $3, $5 }
' <<< "$qstat_out")
if [[ -n $running ]]; then
    ids=$(awk '{ printf "%s%s", sep, $1; sep = "," }' <<< "$running")
    listing=$(awk '{ printf "%s%s %s (%s)", sep, $1, $2, $3; sep = ", " }' <<< "$running")
    notify "STOPPED: another job using the database is already running: $listing. Job $JOB_ID did nothing (no count, nothing queued, no download), so the database is never open in two jobs at once. If that's the auto chain, it carries on by itself; otherwise queue the chain behind it: qsub -hold_jid $ids auto_install.sh"
    exit 2
fi

# --- count successful images ------------------------------------------------

count_out=$(count_success)
if [[ $? -eq 0 && $count_out =~ ^[0-9]+$ ]]; then
    total=$count_out
    total_msg="Total: $(commas "$total") images ($(pct "$total")% of ~50M)."
else
    total=NA
    total_msg="WARNING: could not count successful images (${count_out:-no output, see the .e log}). Continuing without progress numbers."
fi

# --- report on the previous round / safety stops ----------------------------

if [[ -e $STATE_FILE ]]; then
    read -r prev_total prev_start prev_round extra < "$STATE_FILE"
    if ! [[ $prev_total =~ ^([0-9]+|NA)$ && $prev_start =~ ^[0-9]+$ \
            && $prev_round =~ ^[0-9]+$ && -z $extra ]]; then
        notify "STOPPED: $STATE_FILE is malformed ('$(head -c 200 "$STATE_FILE")'). Nothing queued (job $JOB_ID). Fix or delete it, then qsub auto_install.sh"
        exit 2
    fi

    round=$((prev_round + 1))
    elapsed=$((now - prev_start))
    added=""
    report="Round $prev_round finished after $(hours "$elapsed") h"
    if [[ $total != NA && $prev_total != NA ]]; then
        added=$((total - prev_total))
        report+=": +$(commas "$added") images. $total_msg"
    elif [[ $total != NA ]]; then
        report+=" (images added unknown: count failed at the start of round $prev_round). $total_msg"
    else
        report+=". $total_msg"
    fi

    stop_reason=""
    if (( elapsed < MIN_ROUND_SECONDS )); then
        stop_reason="round $prev_round lasted under 1 h -- likely a crash or an empty work queue."
    elif [[ -n $added ]] && (( added <= 0 )); then
        stop_reason="round $prev_round ran a full round but added no images."
    fi
    if [[ -n $stop_reason ]]; then
        notify "$report
STOPPED: $stop_reason Round $round NOT run or queued (job $JOB_ID). Check the previous round's image_install_auto.o*/.e* logs; once fixed: rm -f $STATE_FILE && qsub auto_install.sh"
        exit 2
    fi
else
    round=1
    report="Auto-run starting. $total_msg"
fi

# --- write state, then queue the next round behind this one -----------------

if printf '%s %s %s\n' "$total" "$now" "$round" > "$STATE_FILE"; then
    qsub_out=$(qsub -terse -hold_jid "$JOB_ID" auto_install.sh 2>&1)
    if [[ $? -eq 0 ]]; then
        next="Round $round running as job $JOB_ID; round $((round + 1)) queued as job ${qsub_out} (held until $JOB_ID exits)."
    else
        next="Round $round running as job $JOB_ID. WARNING: could not queue round $((round + 1)) (qsub: ${qsub_out:-no output}). This round runs anyway, but the chain ends after it -- check qstat, then: qsub -hold_jid $JOB_ID auto_install.sh"
    fi
else
    next="Round $round running as job $JOB_ID. WARNING: could not write $STATE_FILE, so round $((round + 1)) was NOT queued. This round runs anyway; the chain ends after it."
fi

notify "$report
$next"

# --- run this round ---------------------------------------------------------

bash image_install_db.sh
