"""Run one third-party ESO reader on a file, for bench/run.sh.

    python others.py <tool> <input.eso> <scratch dir>

The tools do different amounts of work, which the report states:
esoreader and db-eplusout-reader only load the file into memory; opyplus
loads it and writes CSV files.
"""
import sys

tool, eso, scratch = sys.argv[1:4]

if tool == "esoreader":
    import esoreader

    esoreader.read(eso)
elif tool == "db-eplusout-reader":
    from db_eplusout_reader import DBEsoFile

    DBEsoFile.from_path(eso)
elif tool == "opyplus":
    import opyplus

    opyplus.StandardOutput(eso).to_csv(scratch)
else:
    sys.exit("unknown tool " + tool)
