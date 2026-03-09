import csv

input_file = "aisdk-2025-12-11.csv"
output_file = "test_2.csv"
num_rows = 1_000_000

with open(input_file, "r", encoding="utf-8", errors="ignore") as fin, \
     open(output_file, "w", newline="", encoding="utf-8") as fout:

    reader = csv.reader(fin)
    writer = csv.writer(fout)

    # Copy header
    header = next(reader)
    writer.writerow(header)

    for i, row in enumerate(reader, start=1):
        if i > num_rows:
            break
        writer.writerow(row)

print(f"Created test file '{output_file}' with {num_rows} rows.")