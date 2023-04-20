import csv 

def read_file(filename):
    """Reads a CSV file and returns its records as a list of tuples."""
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) # Skip header row
        return [(row[0], row[1], row[2], float(row[3])) for row in reader]

# Read both files into memory and store their records as sets
results1 = set(read_file('results\ScanResult_U2021106D.csv'))
results2 = set(read_file('ScanResults_u2021106d.csv'))

# sort the 2 csv files, create a new copy of the sorted files
results1 = sorted(results1)
results2 = sorted(results2)

# write the sorted files to new csv files
with open('result1_sorted.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(results1)

with open('result2_sorted.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(results2)

# Compare the sets to see if they contain the same records
if results1 == results2:
    print("The two files contain the same records.")
else:
    print("The two files do not contain the same records.")
