# Light DataFrame
A light and user-friendly data structure with convenient filter used to process csv files or tables. <br>

## Quick Example
```
from DataFrame import DataFrame
```
- print orders made in Oct. 2017/2018 sorted by price:
```
df = DataFrame.read_csv("orders.csv")
df.select.where("date").contain("2017-10").Or.contain("2018-10")().sort("price").print()
```

- set field "important" as "False" for all rows whose "price" field value is less than 300:
```
df.update("important", True).where("price").less(300)()
```

- save orders settled in USD with price greater than $1000 to a new csv file:
```
df.select.where("settlement").equal("USD").where("price").greater(1000)().save_csv("some_orders.csv")
```

## Usage

- import
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

- select / add / modify / pop row
```
df_row = df[3]  # get a dataframe formed by the 4th row (return DataFrame)
df_rows = df[[1,3,5,7,9]]  # get a dataframe formed by these 5 rows (return DataFrame)
row = df.rows[2]  # get the third row (return list)
d = df.dict(5)  # get the sixth row in the form of dict (return dict)
dicts = df.dict()  # get all rows in the form of dict (return list)
df.append(new_row)  # append new row
df.append({"col_1": 2, "col_2": 3})  # append new row in the form of dict
df[0] = [34, "True", "Tony"]  # update the first row
df.pop(row_num)  # pop, equivalent to df.rows.pop(row_num=None)
```

- **select / add / modify column**
```
name_list = df["name"]  # get a list
extract_df = df[["description", "value"]]  # get a new DataFrame object with two columns
df["name"] = ["Mary", "Kevin"]  # replace whole column
df["sex"] = ["F", "M"]  # add a new column
```

- **query/update with conditions**

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
df.select.where("id").Not.prefix("AHU")().print(20)

# update the "is_retired" field on condition that the "age" field is greater than 65
df.update("is_retired", True).where("age").greater(65)()
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
df.map(lambda dt: dt.split()[0], "time")
df.map(float, "price")
```

- save csv and append new content to existed csv file
```
df.save_csv(csv_path)
df.append_csv(csv_path)
```