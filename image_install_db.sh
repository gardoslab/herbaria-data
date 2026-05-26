#!/bin/bash -l

# Run the SQLite-tracked image downloader (image_install_db.py).
#
# Prerequisite: build the status database once with init_download_db.sh.
# This job is resumable -- re-submit it any time and it continues from the
# work queue stored in download_status.db.

module load miniconda
module load academic-ml/spring-2026

conda activate spring-2026-pyt

python image_install_db.py

### The command below is used to submit the job to the cluster
### qsub -N image_install_db -l h_rt=48:00:00 -pe omp 16 -P herbdl -m beas -M your_email@bu.edu image_install_db.sh
