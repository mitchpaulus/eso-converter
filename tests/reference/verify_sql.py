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
print("OK  %s: integrity ok, %d values match long CSV" % (db.split("/")[-1], n))
