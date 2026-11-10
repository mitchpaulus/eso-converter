"""Check parquet and esobin outputs against the (already validated) wide CSVs."""
import sys, csv, json, glob, os, math
import numpy as np, pyarrow.parquet as pq, duckdb, polars as pl
base = sys.argv[1]
def csv_table(path):
    with open(path, newline="") as fh:
        r = list(csv.reader(fh))
    return r[0], r[1:]
def same(a, b):
    return (np.isnan(a) & np.isnan(b)) | (a == b)
ok = True
freqs = [os.path.basename(p).split(".")[-2] for p in glob.glob(base + ".*.csv") if not p.endswith(".long.csv")]
hdr = open(base + ".esobin", "rb").read(16)
assert hdr[:8] == b"ESOBIN01"
n = int.from_bytes(hdr[8:16], "little")
raw = open(base + ".esobin", "rb").read()
meta = json.loads(raw[16:16 + n]); b0 = (16 + n + 63) & ~63
bt = {t["frequency"].lower(): t for t in meta["tables"]}
sd = np.dtype([(a, b) for a, b in meta["stamp_dtype"]])
for f in sorted(freqs):
    head, rows = csv_table("%s.%s.csv" % (base, f))
    want = np.array([[float(x) if x != "" else math.nan for x in r[2:]] for r in rows], dtype="f8").reshape(len(rows), len(head) - 2)
    # parquet via pyarrow
    t = pq.read_table("%s.%s.parquet" % (base, f))
    assert t.column_names == head, "names differ"
    assert t.num_rows == len(rows)
    assert t.column(0).to_pylist() == [r[0] for r in rows]
    assert t.column(1).to_pylist() == [r[1] for r in rows]
    got = np.column_stack([t.column(i).to_numpy() for i in range(2, len(head))]) if rows else want
    assert same(got, want).all(), "parquet values differ"
    # duckdb + polars can read it too
    cnt, = duckdb.execute("select count(*) from read_parquet('%s.%s.parquet')" % (base, f)).fetchone()
    assert cnt == len(rows)
    d = pl.read_parquet("%s.%s.parquet" % (base, f)); assert d.shape == (len(rows), len(head))
    if rows:
        s = duckdb.execute("select sum(c) from (select \"%s\" as c from read_parquet('%s.%s.parquet') where not isnan(\"%s\"))" % (head[2].replace('"','""'), base, f, head[2].replace('"','""'))).fetchone()[0]
        w = want[:, 0][~np.isnan(want[:, 0])].sum()
        assert s is None and w == 0 or abs(s - w) <= 1e-9 * max(1, abs(w))
    # esobin
    tb = bt[f]
    assert tb["rows"] == len(rows) and len(tb["columns"]) == len(head) - 2
    arr = np.frombuffer(raw, dtype="<f8", count=tb["rows"] * len(tb["columns"]), offset=b0 + tb["data_offset"]).reshape(len(tb["columns"]), tb["rows"]).T
    assert same(arr, want).all(), "esobin values differ"
    st = np.frombuffer(raw, dtype=sd, count=tb["rows"], offset=b0 + tb["stamps_offset"])
    assert [meta["environments"][e] for e in st["env"]] == [r[0] for r in rows]
    if f == "hourly":
        assert ["%02d/%02d %02d:00:00" % (a, b, c) for a, b, c in zip(st["month"], st["day"], st["hour"])] == [r[1] for r in rows]
    print("OK  %-9s %6d rows x %5d cols  parquet+esobin == csv" % (f, len(rows), len(head) - 2))
