# Utils Directory

This directory contains utility scripts for managing herbarium specimen images, including downloading, processing, organizing, and labeling datasets.

> **Deploying the image downloader?** See [DEPLOYMENT.md](DEPLOYMENT.md) for the
> step-by-step procedure (build the status database once, then run/resume the
> download job).

## Scripts Overview

### Image Download & Installation

#### `image_install_db.py`
**Purpose**: Current script for downloading herbarium specimen images from GBIF (Global Biodiversity Information Facility) multimedia datasets. Downloads **all** images per gbifID, each saved as `<gbifID>-NN.jpg`, with status tracked in a SQLite database. See [DEPLOYMENT.md](DEPLOYMENT.md) for the full procedure.

**Key Features**:
- Parallel downloading with ThreadPoolExecutor (5 workers)
- Host-based rate limiting and circuit breaker pattern
- IIIF (International Image Interoperability Framework) manifest support — one file saved per source URL, highest resolution first
- Automatic image resizing to 1024px max dimension
- Atomic downloads (stream to `.tmp`, length-check, then rename) so a dropped connection never leaves a corrupt file
- SQLite status database for resumable downloads and queryable, classified error tracking — see [`download_db.py`](download_db.py)
- Hierarchical directory organization (3-digit prefix structure)

**Prerequisite**: build the status database once with `python init_download_db.py` before the first run.

**Usage**:
```bash
python image_install_db.py [--db PATH]
```

**Configuration**:
- Input: `/projectnb/herbdl/data/GBIF-F25/multimedia.txt` (ingested once into the database)
- Output: `/projectnb/herbdl/data/GBIF-F25h/`
- Logs: `/projectnb/herbdl/logs/image_install_*.log` (WARNING level and above only — routine successes are recorded in the database, not the log)
- Status: `download_status.db` (default `/projectnb/herbdl/data/GBIF-F25h/download_status.db`)

**Advanced Features**:
- Host cooldown on rate limiting (429 errors): 30 minutes default
- Host cooldown on timeouts: 60 minutes
- Circuit breaker: skips hosts after 500+ errors; state persists across runs in the `hosts` table
- Failures are classified (404, 401, timeout, rate-limited, dropped connection, …); only transient failures are retried, capped at 4 attempts
- Retry strategy with backoff for 500-level errors

#### `image_install_db.sh`
**Purpose**: SCC job submission wrapper for `image_install_db.py`.

**Usage**:
```bash
qsub -N image_install_db -l h_rt=48:00:00 -pe omp 16 -P herbdl -m beas -M your_email@bu.edu image_install_db.sh
```

#### `image_install_parallel.py` (original)
**Purpose**: The original downloader, kept for reference and fallback — superseded by `image_install_db.py`. Downloads **one** image per gbifID (stops at the first URL that succeeds) and tracks progress in flat `processed_ids.txt` / `failed_ids.txt` files.

**Key Features**:
- Parallel downloading with ThreadPoolExecutor (5 workers)
- Host-based rate limiting and circuit breaker pattern
- Duplicate detection across multiple GBIF datasets
- IIIF (International Image Interoperability Framework) manifest support
- Automatic image resizing to 1024px max dimension
- Checkpoint system for resumable downloads
- Failed download tracking
- Hierarchical directory organization (3-digit prefix structure)

**Usage**:
```bash
python image_install_parallel.py [-c COUNTRY_CODE]
```

**Configuration**:
- Input: `/projectnb/herbdl/data/GBIF-F25/multimedia.txt`
- Output: `/projectnb/herbdl/data/GBIF-F25h/`
- Logs: `/projectnb/herbdl/logs/image_install_*.log`
- Checkpoints: `processed_ids.txt`, `failed_ids.txt`

#### `image_install.sh`
**Purpose**: SCC job submission wrapper for the original `image_install_parallel.py`.

**Usage**:
```bash
qsub -N image_install -l h_rt=48:00:00 -pe omp 16 -P herbdl -m beas -M your_email@bu.edu image_install.sh
```

#### `download_db.py`
**Purpose**: SQLite-backed download-status tracking. Imported by the other download scripts — not run directly.

**Why it exists**: replaces the flat `processed_ids.txt` / `failed_ids.txt` files, which recorded only an ID with no reason for failure. The database records, per image URL, whether it succeeded or failed and *why*, so failures are queryable and only transient ones get retried.

**Tables**:
- `images` — one row per source image URL: `status`, `http_status`, `error_type`, `error_detail`, `file_path`, `file_size`, `attempts`
- `gbif_ids` — one row per gbifID; the resumable work queue (`pending` / `partial` / `done` / `failed`)
- `hosts` — per-host error tally and cooldown, so circuit-breaker state survives a restart

#### `init_download_db.py`
**Purpose**: One-time builder for the status database.

**What it does**:
1. Creates the schema
2. Ingests `multimedia.txt` into `images` + `gbif_ids` (so later runs never re-read the 59M-row file)
3. Imports `processed_ids.txt`: renames legacy `<id>.jpg` files to `<id>-00.jpg` for a consistent naming scheme and marks them done. Multi-image gbifIDs are left `partial` so the downloader fetches their remaining images. (`failed_ids.txt` is **not** imported — those IDs get a fresh, tracked retry.)

**Usage**:
```bash
python init_download_db.py                 # build DB + import legacy progress
python init_download_db.py --skip-legacy   # build DB only
python init_download_db.py --reset         # rebuild from scratch
```

#### `status_report.py`
**Purpose**: Report download progress directly from the database — replaces `analyze_image_progress.py`. Every figure is a single indexed SQL query, so it returns in seconds instead of loading ~180 MB of text and re-grouping `multimedia.txt`.

**Reports**: gbifID and per-image progress, failures broken down by type (permanent vs retryable), retry-attempt distribution, worst hosts, and circuit-breaker state. Writes a timestamped `summary_YYYYMMDDHHMM.txt`.

**Usage**:
```bash
python status_report.py [--db PATH] [--output-dir DIR]
```

Ad hoc queries against the database, e.g.:
```sql
-- count each kind of failure
SELECT error_type, COUNT(*) FROM images
WHERE status LIKE 'failed%' GROUP BY error_type ORDER BY 2 DESC;

-- every URL still worth retrying
SELECT gbif_id, url FROM images WHERE status='failed_transient';
```

### Image Processing

#### `image_utils.py`
**Purpose**: Core image processing utilities used by other scripts.

**Functions**:
- `get_file_size_in_mb(file_path)`: Returns file size in megabytes
- `resize_with_aspect_ratio(image_path, output_path, max_size=1600, format="JPEG", quality=85)`:
  - Downscales images preserving aspect ratio
  - Handles alpha channels (RGBA/LA/P with transparency)
  - Converts to RGB JPEG format
  - Returns (changed: bool, final_size: tuple)

**Key Features**:
- Safe alpha channel removal with white background
- Progressive JPEG encoding
- Optimized output
- LANCZOS resampling for high-quality downscaling

#### `resize_images.py`
**Purpose**: Batch resize images in a directory using parallel processing.

**Configuration**:
- Input directory: `/projectnb/herbdl/data/harvard-herbaria/images`
- Target size: Images > 2MB are resized
- Workers: 10 parallel threads
- Log: `image_resize.log`

**Usage**:
```bash
python resize_images.py
```

#### `image_resize.sh`
**Purpose**: SCC job submission wrapper for `resize_images.py`.

**Usage**:
```bash
qsub -l h_rt=24:00:00 -pe omp 10 -P herbdl -m beas -M your_email@bu.edu image_resize.sh
```

#### `compression.sh`
**Purpose**: Compress images to 2MB target size and measure quality degradation using PSNR (Peak Signal-to-Noise Ratio).

**How it works**:
1. Reads images from `./images/` directory
2. Compresses each to 2MB using ImageMagick `convert`
3. Saves compressed versions to `./compressed/` directory
4. Calculates PSNR using FFmpeg and logs to `./logs/`

**Dependencies**: ImageMagick, FFmpeg

**Usage**:
```bash
./compression.sh
```

### Image Organization

#### `reorganize_images.py`
**Purpose**: Reorganize images from flat directory structure into hierarchical structure based on GBIF IDs.

**How it works**:
- Reads images from source directory
- Uses GBIF ID from filename (must be numeric)
- Creates hierarchical structure: `prefix1/prefix2/filename.jpg`
  - prefix1: First 3 digits of GBIF ID
  - prefix2: Digits 4-6 of GBIF ID
- Skips non-numeric filenames

**Configuration**:
- Source: `/projectnb/herbdl/data/GBIF-F25/images`
- Destination: `/projectnb/herbdl/data/GBIF-F25h`
- Supported formats: jpg, jpeg, png, tif, tiff (case-insensitive)

**Example**:
```
Image: 1234567.jpg
→ Moved to: 123/456/1234567.jpg
```

**Usage**:
```bash
python reorganize_images.py
```

### Dataset Labeling

#### `labeling.ipynb`
**Purpose**: Process Kaggle Herbarium 2021 and 2022 metadata to create labeled training/validation datasets.

**What it does**:
1. Loads metadata JSON files from Kaggle Herbarium competitions
2. Extracts taxonomic information (family, genus, species)
3. Generates natural language captions for each specimen
4. Encodes scientific names as numeric labels
5. Creates 80/20 train/validation splits
6. Exports to CSV and JSON formats

**Output Files**:
- `train_2022.csv`, `val_2022.csv` (Herbarium 2022)
- `train_2021.csv`, `val_2021.csv` (Herbarium 2021)
- JSON versions for direct use with HuggingFace datasets

**Columns**:
- `image_id`: Unique identifier
- `filename`: Relative path to image
- `caption`: Natural language description
- `scientificName`: Family + Genus + Species
- `family`, `genus`, `species`: Taxonomic labels
- `scientificNameEncoded`: Numeric label for classification

**Caption Format**:
```
"This is an image of species {species}, in the genus {genus} of family {family}. It is part of the collection of institution {institution}."
```

### Validation & Monitoring

#### `link_check.py`
**Purpose**: Validate image URLs from GBIF multimedia.txt files to identify broken links.

**How it works**:
- Reads multimedia.txt with GBIF IDs and image URLs
- Makes HEAD/GET requests to verify accessibility
- Checks Content-Type headers for valid image types
- Logs invalid links and their GBIF IDs
- Uses parallel processing (10 workers)

**Configuration**:
- Input: `/projectnb/herbdl/data/harvard-herbaria/gbif/multimedia.txt`
- Log: `link_check.log`
- Retry strategy: Up to 5 retries with backoff

**Usage**:
```bash
python link_check.py
```

#### `notifications.py`
**Purpose**: Send push notifications via Pushover API for long-running job monitoring.

**Setup**:
1. Create a `.env` file with:
```
PUSHOVER_API_TOKEN=your_token_here
PUSHOVER_USER_KEY=your_user_key_here
```

**Function**:
```python
send_notification(title, message)
```

**Usage Example**:
```python
from notifications import send_notification
send_notification("Image Installation", "Downloaded 50,000 images")
```

**Integration**: Used by the image download scripts (`image_install_db.py` and `image_install_parallel.py`) to send progress updates every 50,000 images.

## Common Workflows

### 1. Download GBIF Images

See [DEPLOYMENT.md](DEPLOYMENT.md) for the full procedure. In brief:

```bash
# One-time: build the status database (ingest multimedia.txt + import progress)
qsub -N init_download_db -l h_rt=12:00:00 -pe omp 16 -P herbdl init_download_db.sh

# Submit the download job (re-run any time to resume — it reads the work
# queue from the database)
qsub -N image_install_db -l h_rt=48:00:00 -pe omp 16 -P herbdl image_install_db.sh

# Check progress at any time
python status_report.py
```

### 2. Organize Downloaded Images
```bash
# Reorganize flat structure to hierarchical
python reorganize_images.py
```

### 3. Batch Resize Images
```bash
# Submit resize job
qsub -l h_rt=24:00:00 -pe omp 10 -P herbdl image_resize.sh
```

### 4. Create Training Datasets
```bash
# Run labeling notebook
jupyter notebook labeling.ipynb
```

### 5. Validate Image Links
```bash
# Check for broken URLs
python link_check.py
```

## Directory Structures

### Hierarchical Image Storage
Images are organized by GBIF ID prefix for efficient filesystem access. Each
image for a gbifID is saved with a zero-padded index suffix (`-00`, `-01`, ...):
```
/projectnb/herbdl/data/GBIF-F25h/
├── 105/
│   ├── 716/
│   │   ├── 1057161997-00.jpg
│   │   ├── 1057161997-01.jpg
│   ├── 717/
│   │   ├── 1057170001-00.jpg
├── 106/
│   ├── 000/
│   ├── 001/
```

`prefix1` is the first 3 digits of the gbifID, `prefix2` digits 4–6. This
structure prevents issues with directories containing millions of files.

## Dependencies

**Python Libraries**:
- pandas
- PIL (Pillow)
- requests
- scikit-learn (for labeling)
- python-dotenv (for notifications)

**System Tools**:
- ImageMagick (for compression.sh)
- FFmpeg (for compression.sh)

## Notes

- All scripts are designed for use on Boston University's Shared Computing Cluster (SCC)
- Many scripts use parallel processing for performance
- The `download_status.db` SQLite database enables resumable downloads and queryable error tracking; re-running the job simply continues the work queue
- `analyze_image_progress.py` and the `processed_ids.txt` / `failed_ids.txt` files are superseded by the database (`status_report.py`); kept only for historical reference
- Always verify paths before running scripts to avoid data loss
