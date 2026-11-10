"""Turn the hyperfine JSON files written by bench/run.sh into a Markdown
report (stdout) and one combined results.json.

    python3 bench/report.py <eso dir> <results dir> [--warn PERCENT]

With --warn, a GitHub Actions warning is printed to stderr for every case
where the binary under test is more than PERCENT slower than the baseline.
"""
import glob
import json
import os
import statistics
import sys

CASES = ["wide", "wide-1thread", "long", "parquet", "sqlite", "bin"]
WORK = {
    "ReadVarsESO": "ESO to one CSV",
    "esoreader": "load into memory",
    "db-eplusout-reader": "load into memory",
    "opyplus": "load, write CSVs",
}


def fmt_ms(seconds):
    ms = seconds * 1000
    return "%.1f" % ms if ms < 100 else "%.0f" % ms


def main():
    esodir, resdir = sys.argv[1], sys.argv[2]
    warn = float(sys.argv[sys.argv.index("--warn") + 1]) if "--warn" in sys.argv else None

    # (file, case) -> command name -> wall times of every run in every round
    times = {}
    for path in sorted(glob.glob(os.path.join(resdir, "*@*.json"))):
        name, case = os.path.basename(path)[: -len(".json")].split("@")[:2]
        for r in json.load(open(path))["results"]:
            times.setdefault((name, case), {}).setdefault(r["command"], []).extend(r["times"])

    rows, others = [], []
    for (name, case), by in times.items():
        size = os.path.getsize(os.path.join(esodir, name + ".eso"))
        if case.startswith("other-"):
            (t,) = by.values()
            others.append({"file": name, "bytes": size, "tool": case[len("other-") :], "seconds": t[0]})
            continue
        row = {
            "file": name,
            "bytes": size,
            "case": case,
            "runs": len(by["head"]),
            "median": statistics.median(by["head"]),
            "min": min(by["head"]),
        }
        if "base" in by:
            row["base_median"] = statistics.median(by["base"])
            row["base_min"] = min(by["base"])
        rows.append(row)

    rows.sort(key=lambda r: (-r["bytes"], r["file"], CASES.index(r["case"]) if r["case"] in CASES else 99))
    has_base = any("base_median" in r for r in rows)

    print("### esoconv benchmarks\n")
    print(
        "Wall time over %d runs, warm file cache, output to a RAM disk. MB/s is ESO size over"
        " the fastest run.%s\n"
        % (rows[0]["runs"], " Change compares the fastest runs, the steadiest figure on a shared machine." if has_base else "")
    )
    head = ["Input", "ESO MB", "Case", "Median ms", "Min ms", "MB/s"]
    if has_base:
        head += ["Baseline min ms", "Change"]
    print("| " + " | ".join(head) + " |")
    print("|" + "|".join(["---", "---:", "---"] + ["---:"] * (len(head) - 3)) + "|")
    last = None
    for r in rows:
        mb = r["bytes"] / 1e6
        cells = [
            r["file"] if r["file"] != last else "",
            "%.0f" % mb if r["file"] != last else "",
            r["case"],
            fmt_ms(r["median"]),
            fmt_ms(r["min"]),
            "%.0f" % (mb / r["min"]),
        ]
        last = r["file"]
        if has_base:
            if "base_median" in r:
                change = (r["min"] / r["base_min"] - 1) * 100
                r["change_percent"] = change
                cells += [fmt_ms(r["base_min"]), "%+.1f%%" % change]
                if warn is not None and change > warn:
                    print(
                        "::warning::%s %s is %.1f%% slower than the baseline (%s ms vs %s ms)"
                        % (r["file"], r["case"], change, fmt_ms(r["min"]), fmt_ms(r["base_min"])),
                        file=sys.stderr,
                    )
            else:
                cells += ["", ""]
        print("| " + " | ".join(cells) + " |")

    if others:
        f = others[0]["file"]
        ours = next((r["min"] for r in rows if r["file"] == f and r["case"] == "wide"), None)
        print("\n### Other ESO readers\n")
        print("One run each on `%s.eso` (%.0f MB).\n" % (f, others[0]["bytes"] / 1e6))
        print("| Tool | What it does | Time s | vs esoconv wide |")
        print("|---|---|---:|---:|")
        if ours:
            print("| esoconv | ESO to one CSV per frequency | %.3f | 1x |" % ours)
        for o in sorted(others, key=lambda o: o["seconds"]):
            ratio = "%.0fx" % (o["seconds"] / ours) if ours else ""
            print("| %s | %s | %.1f | %s |" % (o["tool"], WORK.get(o["tool"], ""), o["seconds"], ratio))

    with open(os.path.join(resdir, "results.json"), "w") as out:
        json.dump({"esoconv": rows, "others": others}, out, indent=1)


main()
