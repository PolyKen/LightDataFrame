from utils import join, yellow, green
from re import match, search


class DataFrame(object):
    def __init__(self, **kwargs):
        self.name = kwargs.get("name", None)
        self.date = kwargs.get("date", None)
        self.head = kwargs.get("head", [])
        self.rows = kwargs.get("rows", [])

    @staticmethod
    def read_matrix(head, matrix):
        assert len(head) == len(matrix[0]), Exception("{}, {}".format(head, matrix[0]))
        return DataFrame(head=head, rows=matrix)

    @staticmethod
    def read_csv(csv_path):
        with open(csv_path) as file:
            head = file.readline().strip().split(",")
            rows = []
            for r in file.readlines():
                rows.append(r.strip().split(","))
            return DataFrame(head=head, rows=rows)

    def save_csv(self, path):
        with open(path, 'w') as file:
            file.write(join(self.head) + "\n")
            for r in self.rows:
                file.write(join(r) + "\n")

    def to_dict_list(self):
        dict_list = []
        for row in self.rows:
            d = {}
            for i in range(len(self.head)):
                d[self.head[i]] = row[i]
            dict_list.append(d)
        return dict_list

    def empty(self):
        return self.__class__(name=self.name, date=self.date, head=self.head, rows=[])

    def append(self, row):
        assert len(row) == len(self.head)
        self.rows.append(row)

    def print(self, n=-1):
        if len(self.rows) == 0:
            print(green(join(self.head, "\t")))
        else:
            def get_col_width(lst):
                return max(list(map(lambda x: len(str(x)), lst)))

            col_width_list = list(map(lambda col_name: get_col_width(self[col_name]), self.head))

            delta_list = []
            for i in range(len(self.head)):
                delta = col_width_list[i] - len(self.head[i])
                delta_list.append(delta)

            head = ""
            for index, col_name in enumerate(self.head):
                head += col_name
                head += " " * max(delta_list[index] + 2, 2)
            print(green(head))

            i = 0
            for r in self.rows:
                if i == n:
                    break
                else:
                    i += 1
                _row = ""
                for j in range(len(self.head)):
                    head_len = len(self.head[j] + " " * max(delta_list[j] + 2, 2))
                    space_num = head_len - len(str(r[j]))
                    _row += str(r[j]) + " " * space_num
                print(_row)
            print()
        return self

    def __getitem__(self, key):
        if type(key) == str:
            index = self.head.index(key)
            return [r[index] for r in self.rows]
        elif type(key) == int:
            return self.rows[key]
        else:
            raise KeyError

    def __add__(self, other):
        assert self.head == other.head
        self.rows.extend(other.rows)
        return self

    def merge(self, other):
        assert self.head == other.head
        for row in other.rows:
            if row not in self.rows:
                self.rows.append(row)
        return self

    def __sub__(self, other):
        assert self.head == other.head
        self.rows = [row for row in self.rows if row not in other.rows]
        return self

    def map(self, column_name, func):
        if column_name not in self.head:
            raise KeyError

        self[column_name] = list(map(func, self[column_name]))
        return self

    def select(self):
        cls = self.__class__

        class Selector(object):
            def __init__(self, df, field=None):
                self.all_df = cls(head=df.head, rows=df.rows, name=df.name, date=df.date)
                self.keep = df.empty()
                self.df = df
                self.field = field
                self.complement = False

            def empty(self):
                return Selector(df=self.df.empty(), field=self.field)

            def __add__(self, other):
                assert self.field == other.field and self.complement == other.complement
                assert self.all_df == other.all_df

                self.df.merge(other.df)
                self.keep.merge(other.keep)
                return self

            def where(self, field):
                self.field = field
                return self

            def Not(self):
                self.complement = True
                return self

            def Or(self):
                self.keep += self.df
                self.df = self.all_df
                return self

            def equal(self, value):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    try:
                        if r[ind] == value:
                            selected_rows.append(r)
                        else:
                            discarded_rows.append(r)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) == value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) == value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def less(self, value):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    try:
                        if r[ind] < value:
                            selected_rows.append(r)
                        else:
                            discarded_rows.append(r)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) < value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) < value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def greater(self, value):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    try:
                        if r[ind] > value:
                            selected_rows.append(r)
                        else:
                            discarded_rows.append(r)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) > value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) > value:
                                selected_rows.append(r)
                            else:
                                discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def between(self, low, high):
                if self.complement:
                    self.complement = False
                    return self.less(low).Or().equal(low).Or().equal(high).Or().greater(high)
                else:
                    return self.greater(low).less(high)

            def operator(self, opt, value):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    if type(value) == str:
                        expr = "\"{}\"{}\"{}\""
                    else:
                        expr = "{}{}{}"

                    if eval(expr.format(r[ind], opt, value)):
                        selected_rows.append(r)
                    else:
                        discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def prefix(self, pattern):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    if match(pattern, r[ind]):
                        selected_rows.append(r)
                    else:
                        discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def postfix(self, pattern):
                if pattern[-1] != "$":
                    pattern += "$"

                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    if search(pattern, r[ind]):
                        selected_rows.append(r)
                    else:
                        discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def contain(self, substring):
                selected_rows = []
                discarded_rows = []
                ind = self.df.head.index(self.field)
                for r in self.df.rows:
                    if search(substring, r[ind]):
                        selected_rows.append(r)
                    else:
                        discarded_rows.append(r)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.df = cls(csv_path=None, head=self.df.head, rows=selected_rows, name=self.df.name,
                              date=self.df.date)
                return self

            def __call__(self):
                all_df = self.df + self.keep
                if len(all_df.rows) == 0:
                    print(yellow("no row selected"))
                return all_df

        return Selector(self)

    def sort(self, key, reverse=False):
        self.rows = sorted(self.rows, key=lambda x: x[self.head.index(key)], reverse=reverse)
        return self

    def __setitem__(self, column_name, column_list):
        assert len(self.rows) == len(column_list), AssertionError("{}, {}".format(len(self.rows), len(column_list)))
        if column_name not in self.head:
            self.head.append(column_name)
            for i in range(len(self.rows)):
                self.rows[i].append(column_list[i])
        else:
            ind = self.head.index(column_name)
            for i in range(len(self.rows)):
                self.rows[i][ind] = column_list[i]


if __name__ == "__main__":
    df = DataFrame.read_csv(csv_path='test.csv')
    df["value"] = list(map(float, df["value"]))
    df["sp_value"] = list(map(float, df["sp_value"]))
    df.select().where("value").Not().between(10, 79)().sort("value").print(5).sort("value", reverse=True).print(5)
    df.select().where("description").postfix("L3-01").Or().where("description").prefix("AHU-R-B2-02")().print(5)
