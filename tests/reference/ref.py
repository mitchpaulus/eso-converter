"""Slow, obvious reference ESO converter used to validate esoconv."""
import sys, csv, io
FREQS = ["Detailed", "TimeStep", "Hourly", "Daily", "Monthly", "RunPeriod", "Annual"]
KIND = {"Detailed": 2, "TimeStep": 2, "Hourly": 2, "Daily": 3, "Monthly": 4, "RunPeriod": 5, "Annual": 6}

def freq_of(t):
    t = t.split("[")[0].strip().lower().replace(" ", "")
    return {"eachcall": "Detailed", "timestep": "TimeStep", "hourly": "Hourly", "daily": "Daily",
            "monthly": "Monthly", "runperiod": "RunPeriod", "annual": "Annual"}[t]

def dt(freq, s):
    if freq in ("Detailed", "TimeStep", "Hourly"):
        mo, da, hr, em = int(s[1]), int(s[2]), int(s[4]), float(s[6])
        if freq == "Hourly" or em >= 60:
            h, m, sec = hr, 0, 0
        else:
            c = round(em * 100)
            h, m, sec = hr - 1, c // 100, (c % 100) * 60 // 100
        return "%02d/%02d %02d:%02d:%02d" % (mo, da, h, m, sec)
    if freq == "Daily": return "%02d/%02d" % (int(s[1]), int(s[2]))
    if freq == "Monthly": return "%02d" % int(s[1])
    if freq == "RunPeriod": return ""
    return str(int(s[0]))

FIRST = {}; FIRSTDONE = set()
NOLONG = "nolong" in sys.argv
def main(path, outbase, envsel=None):
    lines = open(path, "rb").read().decode("latin-1").replace("\r\n", "\n").split("\n")
    i = 1
    vars_ = {}; order = {f: [] for f in FREQS}
    while lines[i] != "End of Data Dictionary":
        l = lines[i]; i += 1
        a, n, rest = l.split(",", 2)
        if int(a) <= 6: continue
        body, ft = rest.rsplit("!", 1)
        f = freq_of(ft)
        body = body.strip()
        units = body[body.rindex("[") + 1: body.rindex("]")]
        nk = body[:body.rindex("[")].strip()
        key, name = (nk.split(",", 1) + [None])[:2] if "," in nk else ("", nk)
        vars_[int(a)] = (f, key.strip(), name.strip(), units)
        order[f].append(int(a))
    i += 1
    wide = {f: [] for f in FREQS}; long_ = []
    cur = {f: None for f in FREQS}; stamp = {}; env = None; envidx = 0
    def close(f):
        if cur[f] is not None:
            wide[f].append(cur[f]); cur[f] = None
    for l in lines[i:]:
        if l == "End of Data": break
        if not l: continue
        a, rest = l.split(",", 1); a = int(a)
        if a == 1:
            for f in FREQS:
                close(f)
                if f in FIRST: FIRSTDONE.add(f)
            env = rest.split(",")[0].strip(); envidx += 1; continue
        if a <= 6:
            for f in FREQS:
                if KIND[f] == a:
                    close(f)
                    if f in FIRST: FIRSTDONE.add(f)
            stamp[a] = rest.split(","); continue
        f, key, name, units = vars_[a]
        if f not in FIRSTDONE:
            FIRST.setdefault(f, [])
            if a not in FIRST[f]: FIRST[f].append(a)
        if envsel and envidx != envsel: continue
        val = rest.split(",")[0].strip()
        d = dt(f, stamp[KIND[f]])
        if cur[f] is None: cur[f] = (env, d, {})
        cur[f][2][a] = val
        if NOLONG is False: long_.append((env, d, f, key, name, units, val))
    for f in FREQS: close(f)
    # column order: first row of each frequency (whole file), then the rest in dictionary order
    for f in FREQS:
        first = list(FIRST.get(f, []))
        order[f] = first + [a for a in order[f] if a not in set(first)]
    for f in FREQS:
        if not order[f]: continue
        with open("%s.%s.csv" % (outbase, f.lower()), "w", newline="") as fh:
            w = csv.writer(fh, lineterminator="\n")
            hdr = ["Environment", "Date/Time"]
            for a in order[f]:
                _, key, name, units = vars_[a]
                hdr.append(("%s:%s [%s](%s)" % (key, name, units, f)) if key else ("%s [%s](%s)" % (name, units, f)))
            w.writerow(hdr)
            for env_, d, vals in wide[f]:
                w.writerow([env_, d] + [vals.get(a, "") for a in order[f]])
    with open(outbase + ".long.csv", "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["Environment", "Date/Time", "Frequency", "Key", "Variable", "Units", "Value"])
        w.writerows(long_)

main(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].isdigit() else None)
