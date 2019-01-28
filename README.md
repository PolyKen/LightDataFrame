# Light DataFrame
A light and user-friendly data structure used to process csv files or tables.

## Usage

- import: 
```
from DataFrame import DataFrame
```

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

- select / add / pop row
```
row = df[3]  # select 3rd row
rows = df[[1,3,5,7,9]]  # select these 5 rows
df.append(new_row)  # append new row
df.pop(row_num)  # pop, equivalent to df.rows.pop(row_num)
```

- **select / add / modify column**
```
name_list = df["name"]  # get a list
extract_df = df[["description", "value"]]  # get a new DataFrame object with two columns
df["name"] = ["Mary", "Kevin"]  # replace whole column
df["sex"] = ["F", "M"]  # add a new column
```

- **query with conditions**

```
# finding rows where age > 16, sex = "M"
df = df.select.where("age").greater(16).where("sex").equal("M")()

# finding rows where name contains "iphone", price is between 6000 and 9000, release in 2017 or 2018
df = df.select.where("name").contain("iphone").where("price").between(6000, 9000)()
df = df.select.where("release date").contain("2017").Or.contain("2018")()
  
# Note: ".Or" is inferior to ".where(feature).<operator>(cond)", for example,
# df.select.where("A").greater(1).where("B").prefix("cn").Or.where("C").postfix("uw").where("D").less(9)()
# is equivalent to finding rows satisfying "(A > 1 and B prefix "cn") or (C postfix "uw" and D < 9)"

# finding rows where id don't have prefix "AHU", then print 20 results
df.select.where("id").Not.prefix("AHU").print(20)
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