from DataFrame import DataFrame
from itertools import product

if __name__ == "__main__":
    df = DataFrame.read_csv(csv_path='test.csv')
    raw = df.copy()
    raw.select.where("description").prefix("PAU")().print(10)
    df["value"] = list(map(float, df["value"]))
    df["sp_value"] = list(map(float, df["sp_value"]))
    df.select.where("description").prefix("PAU").where("sp_value").equal(70).where("value").Not.between(69.8, 70.2)().sort("value").print(10)
    print(df.select.where("description").contain("R").count())
    # raw.append_csv("test.csv")
    print(df[[3, 5]])
    df[["time", "value"]].print(10, highlight_rows=[1, 2, 3, 4])

    lst = list(range(10))
    mat = [[i, j, k] for i, j, k in product(lst, lst, lst)]
    df = DataFrame.read_matrix(matrix=mat)
    print(df.variance("col_1"))

    a = DataFrame.read_matrix(matrix=[[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    b = a.copy()
    a[2] = [123, 234, 345]
    c = a + b
    d = c - a
    a.print()
    b.print()
    c.print()
    d.print()

    d = {"date": "2019-01-01", "person": "Mark", "value": 20}
    df = DataFrame.read_dict(d)
    df.print()
    df.append({"date": "2019-03-01", "value": 12, "person": "Lua"})
    df.append({"date": "2019-02-02", "value": 23, "person": "Kenny"})
    df.append({"date": "2019-03-12", "value": 15, "person": "Tom"})
    df.print()
    df[1:3].print()
    df.pop()
    df.update("date", "1997-04-21").where("person").equal("Kenny")().print(highlight_rows=2)
    print(df)
    df[1].print()
    df[1].dict(0)["person"] = "Ruby"
    df[1].print()
