# CLAUDE.md — salidaSDDP

## Overview

Windows desktop tool that orchestrates PSR SDDP energy model studies: submits cases to PSR Cloud for execution, downloads results, and post-processes binary output files into Parquet/CSV using the `psr.factory` SDK.

## Architecture

```
main.py                  — Entry point: folder picker → cloud runs → PSRIO post-processing
utils.py                 — UI helpers (tkinter folder chooser with MRU history), logging, Win32 short-path conversion
build.py                 — PyInstaller one-file build script

PSRTools/
  Parameters.py          — Static lookup tables: PSR file ↔ object type mapping, aggregation operations, output formats
  PSRCloudCase.py        — PSRCloudCommand (data class), PSRCloudCommandsList (CSV parser), PSRCloudCase (submit/poll/download via psr.cloud)
  PSRIOCase.py           — PSRIOCase (loads study, writes metadata CSVs, dispatches PSRIO commands), PSRIOCasesList (CSV parser)
  PSRIOCommand.py        — PSRIOCommand: reads .hdr/.bin binary data via psr.factory, applies agent filtering, temporal groupby, factor scaling, saves to parquet/csv

UML/
  salidasSDDP.drawio     — Architecture diagram (draw.io)
```

### Data Flow

1. User selects an output folder containing `psrcloud_commands.csv` and `psrio_commands.csv`.
2. **Cloud phase** — For each row in `psrcloud_commands.csv`, the tool runs/downloads cases on PSR Cloud (threaded). Commands: `Run`, `Download`, `RunDownload`.
3. **Post-processing phase** — For each row in `psrio_commands.csv`, loads the SDDP study with `psr.factory`, reads binary output files, applies temporal aggregation (group-by year/month/day/hour/scenario), and writes results as Parquet or CSV.

### Key Input Files (expected in output folder)

| File | Format |
|------|--------|
| `psrcloud_commands.csv` | `command, version, optimized, psr_study_path, parent_id, id, output_files` |
| `psrio_commands.csv` | `command, psr_study_path, levels, spawn, file, agents` |

### Key Output Files (written to output folder)

- `gen_bus.csv` — Generator-to-bus mapping with technology codes
- `busbar.csv` — Bus name/code listing
- `study.csv` — Study metadata (initial year, stages, simulations)
- `*.parquet` / `*.csv` — Aggregated time-series results per object type

## How to Run

### Prerequisites

- **Python 3.12+** (Windows, uses `ctypes.windll`)
- **Dependencies**: `psr.cloud`, `psr.factory`, `pandas`, `tkinter` (stdlib)
- **Passkey file**: `C:\PSR\passkey.txt` must contain the PSR Cloud authentication key

### Run from source

```powershell
python main.py
```

A GUI folder picker appears. Select the folder containing the two CSV command files.

### Build standalone executable

```powershell
python build.py
```

Produces `dist/main.exe` (one-file PyInstaller bundle with `psr.factory` binaries embedded).

## Key Constraints

- **Windows-only**: Uses `ctypes.windll` for short-path conversion and MessageBox dialogs.
- **Paths must be absolute** in both CSV command files; validated at parse time.
- **PSR Cloud polling**: 60s sleep loop with 30-min status log interval; no configurable timeout.
- **Threading**: Cloud runs execute in threads but share a single `psr.cloud.Client`; downloads run sequentially on the main thread.
- **Hardcoded passkey path**: `C:\PSR\passkey.txt`.
- **Latin-1 encoding** assumed for CSV command files.
- **No CLI arguments**: All configuration is via the CSV files in the selected folder.

## Pending / Inferred TODOs

1. **`psr_cloud_evoc_ibv.xml`** — Should be added to `.gitignore`; `*.xml` pattern would prevent tracking.
2. **Error handling in `PSRIOCommand.process_bin_to_dataframe`** — Currently calls `sys.exit()`; should raise `RuntimeError` instead.
3. **`test.ipynb` / `test0.ipynb`** — Exploratory notebooks present but not integrated into any test suite.
4. **No automated tests** — Should add a `pytest` test suite under `tests/`.
5. **`levels` field parsing** — Needs validation; invalid characters should raise `ValueError`.
6. **`Download` command runs on main thread** — Should be threaded like `Run`/`RunDownload`.
7. **`get_bus` returns `None`** — `get_bus_agents` should skip plants without a bus mapping.
8. **MRU history file** — `load_history()` should prune entries pointing to non-existent directories.
