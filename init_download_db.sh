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

# --processed-file points at the production processed_ids.txt, which lives in
# ljhao's working directory, not this repo.
python init_download_db.py \
    --processed-file /projectnb/herbdl/workspaces/ljhao/herbdl/utils/processed_ids.txt

### The command below is used to submit the job to the cluster
### qsub -N init_download_db -l h_rt=12:00:00 -pe omp 16 -P herbdl -m beas -M your_email@bu.edu init_download_db.sh
