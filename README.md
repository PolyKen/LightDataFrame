# Light DataFrame
A light and user-friendly data structure used to process csv files or tables.

## Usage

- read csv
```
df = DataFrame.read_csv(csv_path)
```

- load matrix(nested list)
```
head = ["name", "time", "value"]
matrix = [["Tom", 1987, 15], ["Jerry", 1993, 32]]
df = DataFrame.read_matrix(head, matrix)
```

- select / add / modify column
```
name_list = df["name"]  # get a list
df["name"] = ["Mary", "Kevin"]  # replace whole column
df["sex"] = ["F", "M"]  # add a new column
```

- query with conditions
