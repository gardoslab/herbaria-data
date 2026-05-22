#!/bin/bash -l

# One-time build of the image-download status database (download_status.db).
#
# This is heavy: it reads the ~59M-row multimedia.txt with pandas and renames
# up to ~13.5M already-downloaded files. Run it as a batch job, not on a login
# node. It only needs to be run once; after that, just (re-)submit
# image_install.sh to download and resume.

module load miniconda
module load academic-ml/spring-2026

conda activate spring-2026-pyt

# The build mode is taken from the qsub command line. Submit with one of:
#   qsub ... init_download_db.sh --reset         # full (re)build from scratch
#   qsub ... init_download_db.sh --legacy-only   # only (re-)run the legacy import
# --processed-file points at the production processed_ids.txt, which lives in
# ljhao's working directory, not this repo.
python init_download_db.py "$@" \
    --processed-file /projectnb/herbdl/workspaces/ljhao/herbdl/utils/processed_ids.txt

# The other big initial run is tracked in
# /projectnb/herbdl/workspaces/tsehou26/herbarium_project/utils/processed_ids.txt and .../failed_ids.txt

### The command below is used to submit the job to the cluster. Use --reset for
### the first build on the new distinct-image schema; --legacy-only for later top-ups:
### qsub -N init_download_db -l h_rt=12:00:00 -pe omp 16 -P herbdl -m beas -M your_email@bu.edu init_download_db.sh --reset
