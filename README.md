# LightDataFrame
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
```
# finding rows where age > 16, sex = "M"
df = df.select().where("age").greater(16).where("sex").equal("M")()

# finding rows where name contains "iphone", price is between 6000 and 9000, release in 2017 or 2018
df = df.select().where("name").contain("iphone").where("price").between(6000, 9000)()
df = df.select().where("release date").contain("2017").Or.contain("2018")()  
# only 1 condition is permitted before "Or()", or it may cause unexpected problem

# finding rows where id don't have prefix "AHU", then print 20 results
df.select().where("id").Not().prefix("AHU").print(20)
```

- sort by column
```
# sort by age
df.sort("age")

# sort by price reversedly
df.sort("price", reverse=True)
```

- concat two DataFrame objects
```
df = df_1 + df_2
```

- merge two DataFrame objects (eliminate all repeated rows)
```
df.merge(another_df)
```

- get difference of two DataFrame objects
```
df = df_1 - df_2
```

- map / apply function on column
```
df.map("time", lambda dt: dt.split()[0])
df.map("price", float)
```

- save csv and append new content to existed csv file
```
df.save_csv(csv_path)
df.append_csv(csv_path)
```