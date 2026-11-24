#!/usr/bin/env python3
"""
STEP 2 - Data preparation
Converts the source workbook (Data.xlsx) or the converted CSV (labour_raw.csv)
into three CSV files, one per data source system:

    DA.csv            rows where Data Source = DA
    Timesheet.csv     rows where Data Source = Timehseet (spelling kept as in source)
    Veh_Activity.csv  rows where Data Source = Veh.Activity

The content is NOT cleaned here - the raw zone keeps the data exactly as the
source systems produced it. Cleaning happens in Spark (step 4).

Usage:  python3 02_prepare_data.py <Data.xlsx | labour_raw.csv> <output_dir>
"""
import csv
import os
import sys

# Source value -> output file name (file names are safe for HDFS folder names)
SOURCE_FILES = {"DA": "DA", "Timehseet": "Timesheet", "Timesheet": "Timesheet",
                "Veh.Activity": "Veh_Activity"}


def read_rows(path):
    """Return (header, rows) from a .csv or .xlsx file."""
    if path.lower().endswith((".xlsx", ".xls")):
        import pandas as pd  # only needed for Excel input
        df = pd.read_excel(path, dtype=str)
        # Excel dates come back as '2017-01-25 00:00:00'; write them in the
        # same dd-Mon-yy form as the CSV export so Spark parses one format
        if "Date" in df.columns:
            d = pd.to_datetime(df["Date"], errors="coerce")
            df["Date"] = d.dt.strftime("%d-%b-%y").where(d.notna(), df["Date"])
        df = df.fillna("")
        return list(df.columns), df.values.tolist()
    with open(path, newline="", encoding="utf-8-sig") as f:   # utf-8-sig strips the BOM
        reader = csv.reader(f)
        header = next(reader)
        return header, list(reader)


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    src, out_dir = sys.argv[1], sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    header, rows = read_rows(src)
    header = [h.strip() for h in header]
    idx = header.index("Data Source")
    print("Source file : {}".format(src))
    print("Total rows  : {:,} (excluding header)".format(len(rows)))

    writers, files, counts = {}, {}, {}
    for row in rows:
        key = SOURCE_FILES.get(row[idx].strip(), "Unknown")
        if key not in writers:
            fh = open(os.path.join(out_dir, key + ".csv"), "w", newline="", encoding="utf-8")
            files[key] = fh
            writers[key] = csv.writer(fh, quoting=csv.QUOTE_MINIMAL)
            writers[key].writerow(header)
            counts[key] = 0
        writers[key].writerow(row)
        counts[key] += 1
    for fh in files.values():
        fh.close()

    print("\nRow counts per file")
    print("{:<20}{:>12}".format("file", "rows"))
    for key in sorted(counts):
        print("{:<20}{:>12,}".format(key + ".csv", counts[key]))
    print("{:<20}{:>12,}".format("TOTAL", sum(counts.values())))
    assert sum(counts.values()) == len(rows), "row count mismatch after split!"
    print("\nCheck passed: split files add up to the source row count.")


if __name__ == "__main__":
    main()
