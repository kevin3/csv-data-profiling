#!/usr/bin/env bash
set -euo pipefail

# AWS Data Profiling Pipeline — Bash Wrapper
# Version: 1.1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-$SCRIPT_DIR/config.yaml}"
PYTHON_ENGINE="$SCRIPT_DIR/python_engine.py"
HTML_REPORTER="$SCRIPT_DIR/html_reporter.py"

# --- Logging Setup ---
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/awdp_$(date +%Y%m%d_%H%M%S).log"
exec 1> >(tee -a "$LOG_FILE")
exec 2> >(tee -a "$LOG_FILE" >&2)

log() {
    local level="$1"
    shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*"
}

# --- YAML Parser (basic) ---
parse_yaml() {
    python3 -c "
import yaml, sys
with open('$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f)
key = '$1'
print(cfg.get(key, ''))
"
}

parse_yaml_nested() {
    python3 -c "
import yaml, sys
with open('$CONFIG_FILE') as f:
    cfg = yaml.safe_load(f)
keys = '$1'.split('.')
val = cfg
for k in keys:
    val = val.get(k, {})
print(val if not isinstance(val, dict) else '')
"
}

# --- Load Config ---
AWS_REGION=$(parse_yaml_nested "aws.region")
S3_BUCKET=$(parse_yaml_nested "aws.s3_bucket")
S3_PREFIX=$(parse_yaml_nested "aws.s3_prefix")
AWS_PROFILE=$(parse_yaml_nested "aws.profile")
TMP_DIR=$(parse_yaml_nested "paths.tmp_dir")
OUTPUT_DIR=$(parse_yaml_nested "paths.output_dir")
PARALLEL_JOBS=$(parse_yaml_nested "processing.parallel_jobs")
CHUNK_SIZE=$(parse_yaml_nested "processing.chunk_size_lines")
CLEANUP_TMP=$(parse_yaml_nested "output.cleanup_tmp")

log INFO "Starting AWS Data Profiling Pipeline"
log INFO "Config: $CONFIG_FILE"
log INFO "AWS Region: $AWS_REGION | Bucket: $S3_BUCKET | Prefix: $S3_PREFIX"

# --- Setup Directories ---
mkdir -p "$TMP_DIR" "$OUTPUT_DIR"

# --- AWS Authentication Check ---
log INFO "Checking AWS credentials..."
if ! AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity --region "$AWS_REGION" >/dev/null 2>&1; then
    log ERROR "AWS authentication failed. Check your credentials and profile."
    exit 1
fi
log INFO "AWS authentication successful."

# --- Pull from S3 ---
S3_URI="s3://$S3_BUCKET/$S3_PREFIX"
log INFO "Syncing data from $S3_URI to $TMP_DIR..."

if ! AWS_PROFILE="$AWS_PROFILE" aws s3 sync "$S3_URI" "$TMP_DIR" --region "$AWS_REGION" --only-show-errors; then
    log ERROR "Failed to sync from S3."
    exit 1
fi

# --- Find CSV Files ---
mapfile -t CSV_FILES < <(find "$TMP_DIR" -maxdepth 1 -name "*.csv" -type f)
if [ ${#CSV_FILES[@]} -eq 0 ]; then
    log ERROR "No CSV files found in $TMP_DIR"
    exit 1
fi

log INFO "Found ${#CSV_FILES[@]} CSV file(s) to process."

# --- Process Each File ---
for CSV_FILE in "${CSV_FILES[@]}"; do
    BASENAME=$(basename "$CSV_FILE" .csv)
    FILE_OUTPUT_DIR="$OUTPUT_DIR/$BASENAME"
    mkdir -p "$FILE_OUTPUT_DIR"

    log INFO "Processing: $BASENAME"

    # Calculate SHA256
    SHA256=$(sha256sum "$CSV_FILE" | awk '{print $1}')
    log INFO "SHA256: $SHA256"
    echo "$SHA256" > "$FILE_OUTPUT_DIR/sha256.txt"

    # Generate preview (first N rows or %)
    TOTAL_LINES=$(wc -l < "$CSV_FILE")
    PREVIEW_ROWS=$(python3 -c "
preview_max = $(parse_yaml_nested 'processing.preview_max_rows')
preview_pct = $(parse_yaml_nested 'processing.preview_percent')
total = $TOTAL_LINES
from math import floor
pct_rows = floor(total * preview_pct / 100)
print(min(preview_max, pct_rows))
")
    head -n "$PREVIEW_ROWS" "$CSV_FILE" > "$FILE_OUTPUT_DIR/preview.csv"
    log INFO "Preview saved: $PREVIEW_ROWS rows"

    # Check file size / line count for chunking decision
    if [ "$TOTAL_LINES" -gt "$CHUNK_SIZE" ]; then
        log INFO "File exceeds chunk size ($CHUNK_SIZE lines). Splitting for parallel processing..."

        CHUNK_DIR="$TMP_DIR/$BASENAME/chunks"
        mkdir -p "$CHUNK_DIR"

        # Split CSV preserving header
        HEADER=$(head -n 1 "$CSV_FILE")
        tail -n +2 "$CSV_FILE" | split -l "$CHUNK_SIZE" - "$CHUNK_DIR/chunk_"

        # Add header to each chunk and rename
        for CHUNK in "$CHUNK_DIR"/chunk_*; do
            CHUNK_NAME=$(basename "$CHUNK")
            {
                echo "$HEADER"
                cat "$CHUNK"
            } > "$CHUNK.tmp"
            mv "$CHUNK.tmp" "$CHUNK.csv"
            rm "$CHUNK"
        done

        # Run parallel profiling on chunks
        mapfile -t CHUNK_FILES < <(find "$CHUNK_DIR" -name "chunk_*.csv" | sort)
        log INFO "Processing ${#CHUNK_FILES[@]} chunks with $PARALLEL_JOBS parallel jobs..."

        printf '%s\n' "${CHUNK_FILES[@]}" | \
            xargs -P "$PARALLEL_JOBS" -I{} bash -c "
                python3 \"$PYTHON_ENGINE\" --config \"$CONFIG_FILE\" --input '{}' --output \"$FILE_OUTPUT_DIR\" --mode chunk
            "

        # Aggregate chunk JSONs
        log INFO "Aggregating chunk results..."
        python3 "$PYTHON_ENGINE" --config "$CONFIG_FILE" --output "$FILE_OUTPUT_DIR" --mode aggregate

    else
        # Single-file full scan
        log INFO "Running full scan profiling..."
        python3 "$PYTHON_ENGINE" --config "$CONFIG_FILE" --input "$CSV_FILE" --output "$FILE_OUTPUT_DIR" --mode full
    fi

    # Generate HTML report for this file
    log INFO "Generating HTML report..."
    python3 "$HTML_REPORTER" --input-dir "$FILE_OUTPUT_DIR" --output "$FILE_OUTPUT_DIR/report.html"

    log INFO "Completed: $BASENAME"
done

# --- Cleanup ---
if [ "$CLEANUP_TMP" = "True" ] || [ "$CLEANUP_TMP" = "true" ]; then
    log INFO "Cleaning up temporary files in $TMP_DIR..."
    rm -rf "$TMP_DIR"/*
fi

log INFO "Pipeline completed successfully. Outputs in: $OUTPUT_DIR"
exit 0