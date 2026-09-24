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
  PSRIOCommand.py        — PSRIOCommand: reads .hdr/.bin binary data via psr.factory, applies agent filtering, resolution-aware temporal groupby, factor scaling, saves to parquet/csv

UML/
  salidasSDDP.drawio     — Architecture diagram (draw.io)
```

### Data Flow

1. User selects an output folder containing `psrcloud_commands.csv` and `psrio_commands.csv`.
2. **Cloud phase** — For each row in `psrcloud_commands.csv`, the tool runs/downloads cases on PSR Cloud (threaded). Commands: `Run`, `Download`, `RunDownload`. On `CloudError`, retries without price optimization before giving up.
3. **Post-processing phase** — For each row in `psrio_commands.csv`, loads the SDDP study with `psr.factory`, reads binary output files, applies resolution-aware temporal aggregation (supports both hourly and block granularity), and writes results as Parquet or CSV.

### Key Input Files (expected in output folder)

| File | Format |
|------|--------|
| `psrcloud_commands.csv` | `command, version, optimized, memory_per_process_ratio, number_of_processes, psr_study_path, parent_id, id, output_files, extensions` (`memory_per_process_ratio`: `2:1` or `4:1`; `number_of_processes`: `64`, `128`, `192`, or `256`; `extensions`: `;`-separated subset of `dat`, `hdr`, `bin`, or empty) |
| `psrio_commands.csv` | `command, psr_study_path, levels, spawn, file, agents` |

### Levels Grammar (temporal aggregation tokens)

| Token | Removes from groupby | Notes |
|-------|---------------------|-------|
| `Y` | year | |
| `M` | month | |
| `D` | day | Only present for hourly resolution |
| `H` | hour | Only present for hourly resolution |
| `B` | block | Only present for block resolution |
| `S` | scenario | Applied after primary aggregation (mean across scenarios) |
| `X` | (none) | Preserves all index levels |

### Spawn Codes

| Code | Spawned file |
|------|-------------|
| `C` | `cmgbus` |
| `D` | `demxba` |
| `T` | `tarimn` |

### Key Output Files (written to output folder)

- `gen_bus.csv` — Generator-to-bus mapping (`genName,genCode,busName,busCode,tech`)
- `busbar.csv` — Bus listing (`busName,busCode,latitude,longitude`)
- `study.csv` — Study metadata (InitialYear, NumberStages, NumberSimulations)
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
- **Threading**: Cloud runs (`Run`/`RunDownload`) execute in threads but share a single `psr.cloud.Client`; `Download` runs sequentially on the main thread.
- **Hardcoded passkey path**: `C:\PSR\passkey.txt`.
- **Latin-1 encoding** assumed for CSV command files.
- **No CLI arguments**: All configuration is via the CSV files in the selected folder.
- **Short-path conversion**: Converts only the parent directory to 8.3 format; final folder name is preserved verbatim.

## Pending / Inferred TODOs

1. **`test0.ipynb`** — Exploratory notebook present but not integrated into any test suite.
2. **No automated tests** — Should add a `pytest` test suite under `tests/`.
3. **`Download` command runs on main thread** — Should be threaded like `Run`/`RunDownload`.
4. **MRU history file** — `load_history()` should prune entries pointing to non-existent directories.
5. **`get_bus_agents` silent empty mapping** — If a generator has no bus mapping (returns `None`), `gen_bus_dict` won't have the key; `get_bus_agents` will `KeyError` on those agents.
