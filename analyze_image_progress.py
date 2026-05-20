#!/usr/bin/env python3
"""
Script to analyze image installation progress by comparing processed_ids.txt
and failed_ids.txt to the multimedia.txt file.

This script answers:
1. How many unique gbifIDs are in multimedia.txt?
2. How many total image URLs are in multimedia.txt?
3. How many gbifIDs have been processed successfully?
4. How many gbifIDs have failed?
5. How many gbifIDs are remaining?
6. Distribution of images per gbifID (to understand multi-image records)

Output is written to summary.txt (by default in the current working directory).
"""

import argparse
import pandas as pd
import os
from collections import Counter

# File paths (matching image_install_parallel.py)
GBIF_MULTIMEDIA_DATA = "/projectnb/herbdl/data/GBIF-F25/multimedia.txt"
CHECKPOINT_BASENAME = "processed_ids.txt"
FAILED_BASENAME = "failed_ids.txt"
OUTPUT_BASENAME = "summary.txt"

STATUS_BASE_DIR = os.getcwd()
OUTPUT_BASE_DIR = os.getcwd()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze image installation progress from checkpoint files."
    )
    parser.add_argument(
        "--status-dir",
        default=STATUS_BASE_DIR,
        help="Directory containing processed_ids.txt and failed_ids.txt "
        f"(default: {STATUS_BASE_DIR!r})",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_BASE_DIR,
        help="Directory for summary.txt "
        f"(default: {OUTPUT_BASE_DIR!r})",
    )
    return parser.parse_args()

def load_id_set(filepath):
    """Load IDs from a text file into a set."""
    if os.path.exists(filepath):
        with open(filepath) as f:
            return {line.strip() for line in f if line.strip()}
    return set()

def main():
    args = parse_args()
    status_dir = os.path.abspath(args.status_dir)
    output_dir = os.path.abspath(args.output_dir)
    checkpoint_file = os.path.join(status_dir, CHECKPOINT_BASENAME)
    failed_file = os.path.join(status_dir, FAILED_BASENAME)
    output_file = os.path.join(output_dir, OUTPUT_BASENAME)
    os.makedirs(output_dir, exist_ok=True)

    # Open output file
    with open(output_file, 'w') as out:
        def write(msg=""):
            """Write to both file and console."""
            out.write(msg + "\n")
            print(msg)

        write("=" * 70)
        write("Image Installation Progress Analysis")
        write("=" * 70)
        write()

        # Load processed and failed IDs
        write("Loading processed and failed IDs...")
        processed_ids = load_id_set(checkpoint_file)
        failed_ids = load_id_set(failed_file)

        write(f"  Processed IDs loaded: {len(processed_ids):,}")
        write(f"  Failed IDs loaded: {len(failed_ids):,}")
        write()

        # Load multimedia.txt
        write("Loading multimedia.txt (this may take a moment)...")
        cols = ['gbifID', 'identifier']
        df = pd.read_csv(
            GBIF_MULTIMEDIA_DATA,
            delimiter="\t",
            usecols=lambda c: c in cols,
            on_bad_lines='skip'
        )
        write(f"  Total rows in multimedia.txt: {len(df):,}")
        write()

        # Group by gbifID to get all identifiers per ID
        grouped = df.groupby('gbifID')['identifier'].apply(list)
        unique_gbif_ids = set(grouped.index.astype(str).tolist())
        total_unique_ids = len(unique_gbif_ids)
        total_image_urls = len(df)

        # Calculate images per gbifID distribution
        images_per_id = grouped.apply(len)
        distribution = Counter(images_per_id)

        write("=" * 70)
        write("OVERALL STATISTICS")
        write("=" * 70)
        write(f"Total unique gbifIDs in multimedia.txt:  {total_unique_ids:,}")
        write(f"Total image URLs in multimedia.txt:      {total_image_urls:,}")
        write(f"Average images per gbifID:               {total_image_urls / total_unique_ids:.2f}")
        write()

        write("=" * 70)
        write("INSTALLATION PROGRESS")
        write("=" * 70)

        # Convert processed/failed to string for comparison
        processed_ids_str = {str(id) for id in processed_ids}
        failed_ids_str = {str(id) for id in failed_ids}

        # Calculate overlaps and remaining
        num_processed = len(processed_ids_str & unique_gbif_ids)
        num_failed = len(failed_ids_str & unique_gbif_ids)
        completed = processed_ids_str | failed_ids_str
        remaining_ids = unique_gbif_ids - completed
        num_remaining = len(remaining_ids)

        write(f"Successfully processed:                  {num_processed:,}")
        write(f"Failed:                                  {num_failed:,}")
        write(f"Total attempted (processed + failed):    {num_processed + num_failed:,}")
        write(f"Remaining (not yet attempted):           {num_remaining:,}")
        write()

        # Progress percentages
        percent_processed = (num_processed / total_unique_ids) * 100
        percent_failed = (num_failed / total_unique_ids) * 100
        percent_remaining = (num_remaining / total_unique_ids) * 100

        write(f"Progress:                                {percent_processed:.2f}%")
        write(f"Failed:                                  {percent_failed:.2f}%")
        write(f"Remaining:                               {percent_remaining:.2f}%")
        write()

        write("=" * 70)
        write("IMAGES PER GBIFID DISTRIBUTION")
        write("=" * 70)
        write("Number of images | Count of gbifIDs")
        write("-" * 35)
        for num_images in sorted(distribution.keys()):
            count = distribution[num_images]
            write(f"{num_images:16d} | {count:,}")
        write()

        # Calculate how many total image URLs we have/need
        write("=" * 70)
        write("IMAGE URL ANALYSIS")
        write("=" * 70)

        # Get image counts for processed, failed, and remaining IDs
        processed_image_count = sum(len(grouped.loc[int(id)])
                                     for id in processed_ids_str & unique_gbif_ids
                                     if int(id) in grouped.index)
        failed_image_count = sum(len(grouped.loc[int(id)])
                                  for id in failed_ids_str & unique_gbif_ids
                                  if int(id) in grouped.index)
        remaining_image_count = sum(len(grouped.loc[int(id)])
                                     for id in remaining_ids
                                     if int(id) in grouped.index)

        write(f"Image URLs for processed gbifIDs:        {processed_image_count:,}")
        write(f"Image URLs for failed gbifIDs:           {failed_image_count:,}")
        write(f"Image URLs for remaining gbifIDs:        {remaining_image_count:,}")
        write(f"Total:                                   {processed_image_count + failed_image_count + remaining_image_count:,}")
        write()

        write("=" * 70)
        write("NOTES")
        write("=" * 70)
        write("- Each gbifID may reference multiple image URLs")
        write("- The script downloads ONE image per gbifID (trying all URLs until one succeeds)")
        write("- 'Processed' means we successfully downloaded at least one image for that ID")
        write("- 'Failed' means all image URLs for that ID failed to download")
        write()

        # Check for any processed/failed IDs not in multimedia.txt
        extra_processed = processed_ids_str - unique_gbif_ids
        extra_failed = failed_ids_str - unique_gbif_ids

        if extra_processed or extra_failed:
            write("=" * 70)
            write("WARNINGS")
            write("=" * 70)
            if extra_processed:
                write(f"⚠️  {len(extra_processed):,} IDs in processed_ids.txt not found in multimedia.txt")
            if extra_failed:
                write(f"⚠️  {len(extra_failed):,} IDs in failed_ids.txt not found in multimedia.txt")
            write()

        write("=" * 70)
        write(f"Output written to: {output_file}")
        write("=" * 70)

if __name__ == "__main__":
    main()
