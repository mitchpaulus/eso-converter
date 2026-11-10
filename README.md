# esoconv

A very fast converter for EnergyPlus `.eso` output files.

It converts a 196 MB / 11.8 million line ESO file in about 50 ms (wide CSV),
where EnergyPlus's own `ReadVarsESO` takes 75 s on the same file and machine.

## Usage

```
esoconv [OPTIONS] <input.eso>

  -f, --format <FMT>   wide (default) | long | parquet | sqlite | bin
  -o, --output <PATH>  output file, or '-' for stdout (CSV formats)
  -e, --env <SEL>      only this environment / run period: 1-based index or
                       case-insensitive part of its title. Repeatable.
      --freq <F>       only this reporting frequency (detailed, timestep,
                       hourly, daily, monthly, runperiod, annual). Repeatable.
  -v, --var <TEXT>     only variables whose 'KEY:Name' contains TEXT. Repeatable.
  -l, --list           list environments and variable counts
      --list-vars      list variables
  -j, --threads <N>    worker threads (default: all cores)
      --timing         print phase timings to stderr
```

Examples:

```sh
esoconv eplusout.eso                       # eplusout.hourly.csv, eplusout.daily.csv, ...
esoconv eplusout.eso --list                # what is in the file
esoconv eplusout.eso -e "run period 1"     # one run period only
esoconv eplusout.eso -f parquet -e 3       # eplusout.hourly.parquet, ...
esoconv eplusout.eso -f sqlite -o out.db
esoconv eplusout.eso --freq hourly -v "zone mean air" -o - | head
```

Without `-o`, outputs are written next to the input with the extension replaced.

## Output formats

All formats use the same row and column definitions.

* **Rows.** One per timestamp per reporting frequency. `Date/Time` is the end
  of the interval in EnergyPlus convention (hours run 01..24):
  `MM/DD HH:MM:SS` for detailed/timestep/hourly, `MM/DD` daily, `MM` monthly,
  empty for run period, `YYYY` for annual. The ESO file does not record the
  year for anything but annual records.
* **Columns.** Named like ReadVarsESO, `KEY:Variable [units](Frequency)`, in
  the order the variables appear in the data.
* **Values.** For daily and longer frequencies the ESO lines also carry
  min/max and their times; only the value itself is converted.

| Format    | Files                         | Notes |
|-----------|-------------------------------|-------|
| `wide`    | one CSV per frequency         | `Environment,Date/Time,<one column per variable>`. Value text is copied from the ESO unchanged. Absent values are empty cells. |
| `long`    | one CSV                       | `Environment,Date/Time,Frequency,Key,Variable,Units,Value`, one line per value, in file order. About 7x the size of the ESO. |
| `parquet` | one file per frequency        | Same columns as `wide`; `DOUBLE` columns, uncompressed, PLAIN encoded. Absent values are NaN. |
| `sqlite`  | one database                  | Tables `environments`, `variables`, `times`, `data(variable_id, time_id, value)` and a view `long` joining them. `data` is keyed on `(variable_id, time_id)`, so per-variable queries need no extra index. |
| `bin`     | one `.esobin` file            | Raw column-major `float64` tables with a JSON header (below). |

With several frequencies, `wide` and `parquet` name files
`<base>.<frequency>.<ext>`. If exactly one `--freq` is given together with
`-o`, the path is used as is.

### `.esobin` layout

```
0        "ESOBIN01"
8        u64 LE: length N of the JSON header
16       JSON header (UTF-8)
B        data area, B = (16 + N) rounded up to a multiple of 64
```

The JSON lists `environments` and `tables`; each table has `frequency`,
`rows`, `columns` (`id`, `key`, `name`, `units`), `stamps_offset` and
`data_offset` (both relative to `B`). At `stamps_offset` are `rows` 12-byte
timestamps (`stamp_dtype` in the header); at `data_offset` are the values,
`float64` little-endian, one column after another. NaN marks an absent value.

```python
import json, numpy as np
raw = np.memmap("eplusout.esobin", mode="r")
n = int.from_bytes(raw[8:16].tobytes(), "little")
meta = json.loads(raw[16:16 + n].tobytes())
base = (16 + n + 63) & ~63
t = next(t for t in meta["tables"] if t["frequency"] == "Hourly")
data = np.frombuffer(raw, "<f8", t["rows"] * len(t["columns"]), base + t["data_offset"])
data = data.reshape(len(t["columns"]), t["rows"])        # data[column, row]
stamps = np.frombuffer(raw, np.dtype([tuple(x) for x in meta["stamp_dtype"]]), t["rows"], base + t["stamps_offset"])
```

## Performance

Input: 196 MB ESO, 11.8 M lines, 1341 hourly + 27 annual variables, 4
environments. Machine: 24 hardware threads, AVX2, NVMe. Times are the
program's own total (`--timing`), median of 7 runs, warm file cache.

| Format  | Output size | Windows (NTFS) | Linux (ext4, WSL2) |
|---------|-------------|----------------|--------------------|
| wide    | 113 MB      | 43–51 ms       | 49 ms              |
| parquet | 95 MB       | 71 ms          | 68 ms              |
| bin     | 95 MB       | 78 ms          | 68 ms              |
| sqlite  | 162 MB      | 117 ms         | 104 ms             |
| long    | 1.33 GB     | 564 ms         | 638 ms             |

One design day only (`-e 1`): under 20 ms. ReadVarsESO on the same file: 75 s.

Parsing alone runs at 3–4 GB/s on one core for the CSV formats (1.4 GB/s when
values are parsed to doubles) and takes 10–16 ms for this file on all cores.
Most of the remaining time is the operating system taking the output.

Two things to know:

* **Run it natively.** From WSL, a file under `/mnt/c` is read at roughly
  60 MB/s, so the same conversion takes about a second there. Use the Windows
  build for files on Windows drives.
* Timings vary with what the disk cache is doing. Right after writing several
  GB, single runs of 2–3 s were observed on Windows.

## How it works

1. The file is memory-mapped and the data dictionary parsed.
2. One SIMD pass finds every environment and timestamp record (lines starting
   with `1,` to `6,`). That gives the environments, row-aligned split points
   for threads, and lets `--env` skip straight to the selected range.
3. The order of variables within a row is read from the first row of each
   frequency. It is constant through the file but is not the dictionary order.
4. Workers take chunks of about 1 MB. The inner loop never parses a variable
   id: it compares the first 8 bytes of the line with the id expected next,
   finds the end of the value with one 32-byte AVX2 compare, and hands the
   value on. Anything unexpected (timestamps, multi-value lines, absent,
   reordered or repeated ids) goes to a general slow path.
5. For CSV the value bytes are copied through, never parsed. For the numeric
   formats values are parsed with the Eisel-Lemire algorithm (`fast-float2`).
6. Output is written in parallel at known offsets: memory-mapped on Windows
   (plain writes are throttled by the cache manager), `pwrite` on Linux.
7. Parquet and SQLite files are produced directly, without their libraries.
   A Parquet column chunk is a small header plus the raw column; the SQLite
   `data` table is one B-tree in column order whose leaf pages are encoded in
   parallel.

## Building

Needs Rust and an x86-64 CPU with AVX2 (2013 or later); `.cargo/config.toml`
sets `target-cpu=x86-64-v3`.

```sh
cargo build --release          # target/release/esoconv(.exe)
cargo test --release
```

`tests/cli.rs` runs the binary on a small real EnergyPlus file
(`tests/data/mixed.eso`, all seven frequencies) and compares against a naive
reference parser. `tests/reference/` has the Python scripts used for the
deeper checks during development: an independent converter (`ref.py`), a
Parquet/esobin check with pyarrow, DuckDB and polars (`verify_num.py`), and a
SQLite check including `PRAGMA integrity_check` (`verify_sql.py`).

## Limitations

* Min/max fields of daily and longer frequencies are not converted.
* x86-64 with AVX2 only. There is no AVX-512 path.
* `parquet`, `sqlite` and `bin` hold the parsed values in memory (about twice
  8 bytes per value); the CSV formats stream.
* A Parquet table is written as one row group with one page per column, which
  limits a table to about 268 million rows.
* The input must be a regular file (it is memory-mapped).
