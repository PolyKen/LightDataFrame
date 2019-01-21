from utils import join, yellow, green, timer, now
from re import match, search


class DataFrame(object):
    def __init__(self, *args, **kwargs):
        skip = False

        head = kwargs.get("head", [])
        rows = kwargs.get("rows", [])
        name = kwargs.get("name", None)
        date = kwargs.get("date", now(fmt="%m-%d"))

        for arg in args:
            if type(arg) == DataFrame:
                name = arg.name
                date = arg.date
                head = arg.head
                rows = arg.rows
                skip = True
                break

        if not skip:
            for k, v in kwargs.items():
                if type(v) == DataFrame:
                    name = v.name
                    date = v.date
                    head = v.head
                    rows = v.rows
                    break

        self.name = name
        self.date = date
        self.head = head
        self.rows = rows

    def copy(self):
        return self.__class__(name=self.name, date=self.date, head=self.head, rows=self.rows)

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

    def append_csv(self, path):
        with open(path, 'a') as file:
            for r in self.rows:
                file.write(join(r) + "\n")

    def to_dicts(self):
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
        if type(row) == list:
            assert len(row) == len(self.head)
            self.rows.append(row)
        if type(row) == dict:
            r = [0 for _ in range(len(self.head))]
            for k, v in row.items():
                try:
                    index = self.head.index(k)
                except ValueError:
                    continue
                r[index] = v
            self.rows.append(r)

    def pop(self, row_num):
        return self.rows.pop(row_num)

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

    def __sub__(self, other):
        assert self.head == other.head
        self.rows = [row for row in self.rows if row not in other.rows]
        return self

    def __len__(self):
        return len(self.rows)

    @timer
    def merge(self, other):
        assert self.head == other.head
        for row in other.rows:
            if row not in self.rows:
                self.rows.append(row)
        return self

    def map(self, column_name, func):
        if column_name not in self.head:
            raise KeyError

        self[column_name] = list(map(func, self[column_name]))
        return self

    @property
    def select(self):
        def detail(func):
            @timer
            def wrapper(*args, **kwargs):
                wrapper.__name__ = func.__name__
                total_num = len(args[0].selected)
                res = func(*args, **kwargs)
                num_selected = len(args[0].selected)
                num_kept = len(args[0].kept)
                print("[{}] {} out of {} row(s) selected, {} row(s) kept".format(func.__name__, num_selected, total_num,
                                                                                 num_kept))
                return res

            return wrapper

        class Filter(object):
            def __init__(self, df, field=None):
                self.all_df = set(range(len(df.rows)))
                self.kept = set()
                self.selected = set(range(len(df.rows)))
                self.df = df
                self.field = field
                self.complement = False

            def __add__(self, other):
                assert self.field == other.field and self.complement == other.complement
                assert self.df == other.df

                self.kept = self.kept.union(other.keep)
                self.selected = self.selected.union(other.selected)
                return self

            def __call__(self):
                all_df = self.df.empty()
                self.selected = self.selected.union(self.kept)
                for i in self.selected:
                    all_df.append(self.df.rows[i])
                if len(all_df.rows) == 0:
                    print(yellow("no row selected"))
                else:
                    print("{} out of {} row(s) selected".format(len(all_df.rows), len(self.df.rows)))
                return all_df

            def empty(self):
                return Filter(df=self.df.empty(), field=self.field)

            def where(self, field):
                self.field = field
                return self

            @property
            def Not(self):
                self.complement = True
                return self

            @property
            def Or(self):
                self.kept = self.kept.union(self.selected)
                self.selected = self.all_df.difference(self.kept)
                return self

            @detail
            def equal(self, value):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    try:
                        if r[ind] == value:
                            selected_rows.add(i)
                        else:
                            discarded_rows.add(i)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) == value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) == value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def less(self, value):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    try:
                        if r[ind] < value:
                            selected_rows.add(i)
                        else:
                            discarded_rows.add(i)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) < value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) < value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def greater(self, value):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    try:
                        if r[ind] > value:
                            selected_rows.add(i)
                        else:
                            discarded_rows.add(i)
                    except TypeError:
                        if type(value) == str:
                            if str(r[ind]) > value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)
                        elif type(value) == int or type(value) == float:
                            if float(r[ind]) > value:
                                selected_rows.add(i)
                            else:
                                discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def between(self, low, high):
                if self.complement:
                    self.complement = False
                    origin_all_df = self.all_df.copy()
                    self.all_df = self.selected.copy()
                    bet = self.less(low).Or.equal(low).Or.equal(high).Or.greater(high)
                    bet.all_df = origin_all_df
                    return bet
                else:
                    return self.greater(low).less(high)

            @detail
            def operator(self, opt, value):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i, r in enumerate(self.df.rows):
                    if type(value) == str:
                        expr = "\"{}\"{}\"{}\""
                    else:
                        expr = "{}{}{}"

                    if eval(expr.format(r[ind], opt, value)):
                        selected_rows.add(i)
                    else:
                        discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def prefix(self, pattern):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    if match(pattern, r[ind]):
                        selected_rows.add(i)
                    else:
                        discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def postfix(self, pattern):
                if pattern[-1] != "$":
                    pattern += "$"

                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    if search(pattern, r[ind]):
                        selected_rows.add(i)
                    else:
                        discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            @detail
            def contain(self, substring):
                selected_rows = set()
                discarded_rows = set()
                ind = self.df.head.index(self.field)
                for i in self.selected:
                    r = self.df.rows[i]
                    if search(substring, r[ind]):
                        selected_rows.add(i)
                    else:
                        discarded_rows.add(i)

                if self.complement:
                    selected_rows, discarded_rows = discarded_rows, selected_rows
                    self.complement = False

                self.selected = selected_rows
                return self

            def count(self):
                all_selected = self.selected.union(self.kept)
                return len(all_selected)

        return Filter(self)

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
    raw = df.copy()
    raw = raw.select().where("description").prefix("PAU")()
    df["value"] = list(map(float, df["value"]))
    df["sp_value"] = list(map(float, df["sp_value"]))
    df.select().where("description").prefix("PAU").where("sp_value").equal(70).where("value").Not().between(69.8, 70.2)().sort("value")
    raw.append_csv("test.csv")
