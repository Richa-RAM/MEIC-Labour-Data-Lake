#!/usr/bin/env python3
"""
STEP 7 (part 1) - Timesheet replay "producer"

Simulates timesheets arriving in real time: it takes the Timesheet.csv from
step 2, sorts it by date and drops a small CSV file ("micro-batch") into the
HDFS landing folder every few seconds. The Spark Structured Streaming job
(stream_ingest.py) picks each file up as it arrives.

Usage:
  python3 replay_timesheets.py <Timesheet.csv> <hdfs_landing_dir> [rows_per_batch] [seconds_between] [max_batches]
Example:
  python3 replay_timesheets.py ~/meic_staging/Timesheet.csv /datalake/landing/timesheet_stream 500 5 12
"""
import csv
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    src, landing = sys.argv[1], sys.argv[2]
    rows_per_batch = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    pause = float(sys.argv[4]) if len(sys.argv) > 4 else 5
    max_batches = int(sys.argv[5]) if len(sys.argv) > 5 else 12

    with open(src, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    d = header.index("Date")
    rows.sort(key=lambda r: datetime.strptime(r[d], "%d-%b-%y"))

    hdfs = os.path.join(os.environ.get("HADOOP_HOME", ""), "bin", "hdfs")
    hdfs = hdfs if os.path.exists(hdfs) else "hdfs"
    subprocess.check_call([hdfs, "dfs", "-mkdir", "-p", landing])

    sent = 0
    tmp_dir = tempfile.mkdtemp(prefix="ts_stream_")
    for b in range(max_batches):
        batch = rows[b * rows_per_batch:(b + 1) * rows_per_batch]
        if not batch:
            break
        name = "timesheet_batch_{:04d}.csv".format(b + 1)
        local = os.path.join(tmp_dir, name)
        with open(local, "w", newline="", encoding="utf-8") as out:
            w = csv.writer(out)
            w.writerow(header)
            w.writerows(batch)
        # hdfs -put writes to a temporary ._COPYING_ name and renames when complete,
        # so Spark never reads a half-written file
        subprocess.check_call([hdfs, "dfs", "-put", "-f", local, landing + "/" + name])
        sent += len(batch)
        print("[{:%H:%M:%S}] sent {}: {} rows ({} .. {}), total sent {}".format(
            datetime.now(), name, len(batch), batch[0][d], batch[-1][d], sent), flush=True)
        time.sleep(pause)
    print("Replay finished: {} rows in {} batches.".format(sent, b + 1))


if __name__ == "__main__":
    main()
