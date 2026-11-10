"""Check the SQLite output against the validated long CSV."""
import sys, csv, sqlite3, math
db, longcsv = sys.argv[1], sys.argv[2]
con = sqlite3.connect(db)
ic = con.execute("pragma integrity_check").fetchall()
assert ic == [("ok",)], ic[:5]
with open(longcsv, newline="") as fh:
    r = csv.reader(fh); next(r)
    want = {}
    for env, d, f, key, name, units, val in r:
        v = float(val)
        if not math.isnan(v): want[(env, d, f, key, name, units)] = v     # last value wins, as in wide
n = 0
for env, d, f, key, name, units, val, ty in con.execute("select *, typeof(value) from long"):
    assert ty == "real", ty
    w = want[(env, d, f, key, name, units)]
    assert w == val and math.copysign(1, w) == math.copysign(1, val), (w, val)
    n += 1
assert n == len(want), (n, len(want))
# primary-key lookups work (uses the b-tree, not a scan)
vid, = con.execute("select variable_id from data order by variable_id desc limit 1").fetchone() or (None,)
if vid is not None:
    a = con.execute("select count(*), min(time_id), max(time_id) from data where variable_id=?", (vid,)).fetchone()
    b = con.execute("select count(*), min(time_id), max(time_id) from data where variable_id+0=?", (vid,)).fetchone()
    assert a == b, (a, b)
    plan = con.execute("explain query plan select * from data where variable_id=? and time_id=?", (vid, 5)).fetchall()
    assert "SEARCH" in plan[0][3], plan
    tid = a[2]
    assert con.execute("select count(*) from data where variable_id=? and time_id=?", (vid, tid)).fetchone() == (1,)
# a lookup by variable name and period goes through the indexes, never a scan of data
plan = con.execute("explain query plan select d.value from variables v join data d on d.variable_id=v.id "
                   "join times t on t.id=d.time_id where v.name=? and t.month=7", ("x",)).fetchall()
detail = [p[3] for p in plan]
assert any("variables_name" in d for d in detail) and not any(d.startswith("SCAN d") for d in detail), detail
for idx, table, cols in (("variables_name", "variables", "name, key, id"),
                         ("times_period", "times", "environment_id, frequency, month, day, id")):
    a = con.execute("select %s from %s indexed by %s order by %s" % (cols, table, idx, cols)).fetchall()
    b = con.execute("select %s from %s not indexed order by %s" % (cols, table, cols)).fetchall()
    assert a == b, idx
# the stored statistics are what ANALYZE itself computes
ours = sorted(con.execute("select * from sqlite_stat1").fetchall(), key=repr)
con.execute("analyze")
assert ours == sorted(con.execute("select * from sqlite_stat1").fetchall(), key=repr), ours
con.rollback()
print("OK  %s: integrity ok, %d values match long CSV" % (db.split("/")[-1], n))
