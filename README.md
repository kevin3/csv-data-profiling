# csv-data-profiling
### `requirements.txt`

```
duckdb>=0.9.0
pyyaml>=6.0
pandas>=2.0.0
```

---

## Installation & Usage

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure config.yaml with your S3 bucket and AWS region

# 3. Make scripts executable
chmod +x bash_wrapper.sh python_engine.py html_reporter.py

# 4. Run the pipeline
./bash_wrapper.sh config.yaml
```

---

## Output Structure

```
output/
└── my_data_file/
    ├── sha256.txt
    ├── preview.csv
    ├── summary.json
    ├── summary.csv
    ├── rejected.csv          # (only if malformed rows found)
    ├── chunks/               # (only if chunked)
    │   ├── chunk_aa.json
    │   └── chunk_ab.json
    └── report.html
```

---

## Key Design Decisions

| Decision | Rationale |
|:---|:---|
| **DuckDB in-memory** | Fast analytical queries without external DB setup |
| **Bash orchestration** | Native Linux job control, `xargs -P` for parallelism, no heavy dependencies |
| **YAML config** | Single source of truth; human-readable; easy to version control |
| **Chunk + aggregate** | DuckDB's columnar engine handles chunks efficiently; aggregation is lightweight |
| **Rejects table** | Uses DuckDB native `rejects_table` instead of non-existent `REJECTED TRUE` |
| **Dark-themed HTML** | Professional data-tool aesthetic; easy to read in terminal/browser |
| **SHA256 + preview** | Integrity verification + quick human inspection before full profiling |

---

```markdown
# Software Requirements Specification (SRS)

**Project Name:** AWS Data Profiling Pipeline (AWDPP)  
**Version:** 1.1
**Status:** Corrected / Ready for Development

## 1. Introduction
### 1.1 Purpose
The primary goal of the AWS Data Profiling Pipeline (AWDPP) is to provide a secure, CLI-based mechanism on Linux systems to pull raw CSV data from S3 buckets, profile its contents using DuckDB, and generate structured reports for data quality assurance and data science discovery.

### 1.2 Scope
The solution consists of three components:
1. **Bash Wrapper:** Manages AWS interaction, file movement, logging, and orchestrates job execution.
2. **Python Core Engine:** Contains the DuckDB integration, SHA256 calculation, and summary logic.
3. **Reporting Script:** Generates an HTML overview from generated JSON files.

### 1.3 Intended Users
* Data Engineers performing ETL/Data Quality checks.
* Data Scientists preparing CSV data for analysis.

---

## 2. Functional Requirements (What the system MUST DO)

### 2.1 Input Management (Data Ingestion)
**FR-1.1:** The Bash wrapper SHALL be responsible for authenticating via AWS CLI and pulling files from designated S3 paths to `/tmp/awdp`.
**FR-1.2:** The system SHALL support single CSV input for v1.0. The Python engine SHALL use file-path abstraction to enable future Parquet compatibility.
**FR-1.3:** Upon successful ingestion into `/tmp/awdp`, the system SHALL calculate and output a SHA256 hash of the raw CSV file.
**FR-1.4:** The system SHALL output the first 10,000 rows (or 10.1% of total rows, whichever is SMALLER) of the CSV file as `preview.csv` for quick inspection.

### 2.2 Data Profiling & Processing
**FR-2.1:** The Python engine SHALL connect to the data via an in-memory DuckDB instance.
**FR-2.2:** The Python engine SHALL execute profiling using DuckDB's `SUMMARIZE SELECT * FROM 'file.csv'` to generate data summaries.
**FR-2.3:** For high performance/large files: The system SHALL optionally split the input CSV files by line count, run single-column profiles on each part, and aggregate results.
**FR-2.4:** Full Scan: DuckDB SHALL run the full CSV against the in-memory instance to generate a comprehensive summary.
**FR-2.5:** All profiling summaries SHALL be saved as JSON objects.

### 2.3 Output & Reporting
**FR-3.1:** Profiling results SHALL be saved to `summary.csv` for easy loading into spreadsheets.
**FR-3.2:** For parallel processing results: Each job SHALL save its individual summary to a uniquely named JSON file (e.g., `table1/report.json`).
**FR-3.3:** A dedicated HTML Reporter script SHALL read all generated JSON files from the designated report folder, aggregate the data, and output a human-readable `report.html`.

---

## 3. Non-Functional Requirements (How the system SHOULD perform)

### 3.1 Performance & Scalability
**NFR-1.1:** The system SHALL support parallel processing of multiple jobs, where the wrapper controls the number of concurrent jobs (e.g., 4, 8, 16) to balance memory usage.
**NFR-1.2:** The Bash wrapper SHALL be flexible enough to split large CSV files into chunks, allowing parallel execution against multiple parts without requiring a full memory load for each part.
**NFR-1.3:** The entire pipeline SHALL operate with an estimated minimum RAM allowance of 8GB per job.

### 3.2 Error Handling & Resiliency
**NFR-2.1:** If specific rows in the CSV are detected as malformed and cannot be read by DuckDB, the system SHALL utilize DuckDB's `ignore_errors=true` with `rejects_table` feature and output a `rejected.csv` file containing the bad rows.
**NFR-2.2:** If an AWS credential/connection failure occurs, the Bash wrapper SHALL exit with a non-zero status code (1) and log the specific AWS error.
**NFR-2.3:** The Python logic SHALL gracefully handle failure if the input file is missing from the designated `/tmp/awdp` path.

### 3.3 Configuration & Portability
**NFR-3.1:** All run-time configurable parameters (AWS region, S3 bucket path, parallel job count) SHALL be stored in a YAML configuration file.
**NFR-3.2:** The entire suite of scripts (Bash + Python) SHALL be executed within the Linux bash environment and should not require a heavy desktop dependency environment (ideally suited for headless servers).
**NFR-3.3:** The system SHALL clean up temporary files in `/tmp/awdp` after successful processing, retaining only the final outputs (JSON, CSV, HTML).
**NFR-3.4:** The system SHALL write structured logs to a rotating log file with levels (INFO, WARN, ERROR).

---

## 4. Technical Requirements Traceability Matrix (Summary)

| Feature | Component | Key Tool | Requirements |
|:---|:---|:---|:---|
| File Retrieval | Bash Wrapper | `aws cli` | Secure pull to `/tmp/awdp` (FR-1.1) |
| Hashing/Preview | Python Engine | `hashlib` (SHA256) | Integrity check (FR-1.3, FR-1.4) |
| Data Profiling | Python Engine | `DuckDB` (In-Memory) | Summarization (FR-2.1, FR-2.2, FR-2.4) |
| Rejected Rows | Python Engine | `DuckDB` (`rejects_table`) | Robust error handling (NFR-2.1) |
| Aggregated Output | Python Engine | `JSON/CSV` | JSON and CSV output formats (FR-2.5, FR-3.1) |
| Final Report | Reporter Script | `HTML/Jinja2` | Combine JSONs into report.html (FR-3.3) |
| Job Management | Bash Wrapper | `xargs/parallel` | Parallel control & chunking (NFR-1.1, NFR-1.2) |
| Configuration | System | `YAML` | Centralized settings (NFR-3.1) |
| Logging | Bash + Python | `logger` / `logging` | Structured logs (NFR-3.4) |
| Cleanup | Bash Wrapper | `rm` | Temp file cleanup (NFR-3.3) |

---
