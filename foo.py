import csv
import pyarrow as pa
import pyarrow.compute as pc

animal = pa.array(["sheep", "cows", "horses", "foxes", "sheep"], type=pa.string())
count = pa.array([12, 5, 2, 1, 10], type=pa.int8())
year = pa.array([2022, 2022, 2022, 2022, 2021], type=pa.int16())

# Creating a table from arrays
table = pa.Table.from_arrays([animal, count, year], names=['animal', 'count', 'year'])
print(table)

count_animals = pc.value_counts(table['animal'])
print(count_animals)

filtered_table = table.filter(pc.greater(table['count'], 2))
print(filtered_table)

sum_filtered = pc.sum(filtered_table['count'])
print(sum_filtered.as_py())