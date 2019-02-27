from utils import join, yellow, green, timer, now
from re import match, search
import random


class DataFrame(object):
    def __init__(self, *args, **kwargs):
        skip = False

        head = kwargs.get("head", [])
        rows = kwargs.get("rows", [])
        name = kwargs.get("name", None)
        date = kwargs.get("date", now(fmt="%m-%d"))

        verbose = kwargs.get("verbose", False)

        for arg in args:
            if type(arg) == DataFrame:
                name = arg.name
                date = arg.date
                head = arg.head
                rows = arg.rows
                verbose = arg.verbose
                skip = True
                break

        if not skip:
            for k, v in kwargs.items():
                if type(v) == DataFrame:
                    name = v.name
                    date = v.date
                    head = v.head
                    rows = v.rows
                    verbose = v.verbose
                    break

        self.name = name
        self.date = date
        self.head = head
        self.rows = rows

        self.verbose = verbose

    def __getitem__(self, key):
        if type(key) == str:
            index = self.head.index(key)
            return [r[index] for r in self.rows]
        elif type(key) == int:
            return self.rows[key]
        elif type(key) == list:
            if type(key[0]) == int:
                lists = []
                for ind in key:
                    lists.append(self.rows[ind])
                return lists
            elif type(key[0]) == str:
                df = self.__class__(name=self.name, date=self.date, head=[], rows=[[] for _ in range(len(self))])
                for col_name in key:
                    index = self.head.index(col_name)
                    df[col_name] = [r[index] for r in self.rows]
                return df
        else:
            print("{}(type: {}) is not a column name or row index".format(key, type(key)))
            raise KeyError

    def __setitem__(self, column_name_or_row_index, column_or_row):
        key = column_name_or_row_index
        value = column_or_row
        assert len(self.rows) == len(value), AssertionError("{}, {}".format(len(self.rows), len(value)))
        if type(key) == str:
            if key not in self.head:
                self.head.append(key)
                for i in range(len(self.rows)):
                    self.rows[i].append(value[i])
            else:
                ind = self.head.index(key)
                for i in range(len(self.rows)):
                    self.rows[i][ind] = value[i]
        elif type(key) == int:
            assert len(self.head) == len(value)
            self.rows[key] = value
        else:
            print("{}(type: {}) is not a column name or row index".format(key, type(key)))
            raise KeyError

    def __add__(self, other, inplace=False):
        assert self.head == other.head
        if inplace:
            self.rows.extend(other.rows)
            return self
        else:
            rows = []
            for row in self.rows:
                rows.append(row)
            for row in other.rows:
                rows.append(row)
            return self.__class__(name=self.name, date=self.date, head=self.head.copy(), rows=rows)

    def __sub__(self, other, inplace=False):
        assert self.head == other.head
        rows = [row for row in self.rows if row not in other.rows]
        if inplace:
            self.rows = rows
            return self
        else:
            return self.__class__(name=self.name, date=self.date, head=self.head.copy(), rows=rows)

    def __len__(self):
        return len(self.rows)

    def __str__(self):
        abstract = "<DataFrame object> name: {}, {} row(s)".format(self.name, len(self))
        self.print()
        return abstract

    def copy(self):
        rows = []
        for row in self.rows:
            rows.append(row)
        return self.__class__(name=self.name, date=self.date, head=self.head.copy(), rows=rows)

    @staticmethod
    def read_dict(d, **kwargs):
        name = kwargs.get("name", None)
        date = kwargs.get("date", None)
        head, row = [], []
        for k, v in d.items():
            head.append(k)
            row.append(v)
        return DataFrame(name=name, date=date, head=head, rows=[row])

    @staticmethod
    def read_matrix(matrix, **kwargs):
        head = kwargs.get("head", ["col_{}".format(i + 1) for i in range(len(matrix[0]))])
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

    def dict(self, row_num=None):
        if row_num is None:
            dict_list = []
            for row in self.rows:
                d = {}
                for i in range(len(self.head)):
                    d[self.head[i]] = row[i]
                dict_list.append(d)
            return dict_list
        else:
            row = self.rows[row_num]
            d = {}
            for i in range(len(self.head)):
                d[self.head[i]] = row[i]
            return d

    def empty(self):
        return self.__class__(name=self.name, date=self.date, head=self.head.copy(), rows=[])

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

    def pop(self, row_num=-1):
        return self.rows.pop(row_num)

    def print(self, n=-1, highlight_rows=None):
        if highlight_rows is None:
            highlight_rows = []
        elif type(highlight_rows) == int:
            highlight_rows = [highlight_rows]

        if len(self.rows) == 0:
            print(green(join(self.head, "\t")))
        else:
            def get_col_width(lst):
                return max(list(map(lambda x: len(str(x)), lst)))

            col_width_list = list(map(lambda col: get_col_width(self[col]), self.head))

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
                if i - 1 in highlight_rows:
                    print(yellow(_row))
                else:
                    print(_row)
            print()
        return self

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

    def mean(self, column_name_or_row_index):
        lst = self[column_name_or_row_index]
        return sum(lst) / len(lst)

    def variance(self, column_name_or_row_index):
        lst = self[column_name_or_row_index]
        mean = self.mean(column_name_or_row_index)
        return sum(list(map(lambda x: (x - mean) ** 2, lst))) / len(lst)

    def sample(self, num=None, proportion=None):
        if num is not None:
            indices = random.sample(range(len(self)), num)
        elif proportion is not None:
            indices = random.sample(range(len(self)), int(len(self) * proportion))
        else:
            indices = random.sample(range(len(self)), 0.1)
        return self[indices]

    def sort(self, key, reverse=False):
        self.rows = sorted(self.rows, key=lambda x: x[self.head.index(key)], reverse=reverse)
        return self

    def update(self, col_name, new_value):
        assert (col_name is None and new_value is None) or (col_name is not None and new_value is not None)
        _col_name = col_name
        _value = new_value
        verbose = self.verbose

        def detail(func):
            if not verbose:
                def wrapper(*args, **kwargs):
                    wrapper.__name__ = func.__name__
                    return func(*args, **kwargs)

                return wrapper

            @timer
            def wrapper(*args, **kwargs):
                wrapper.__name__ = func.__name__
                total_num = len(args[0].selected)
                res = func(*args, **kwargs)
                num_selected = len(args[0].selected)
                num_kept = len(args[0].kept)
                report_text = "[{}] {} out of {} row(s) selected, {} row(s) kept"
                print(report_text.format(func.__name__, num_selected, total_num, num_kept))
                return res

            return wrapper

        class Filter(object):
            def __init__(self, df, field=None):
                self.all_df = set(range(len(df)))
                self.kept = set()
                self.selected = set(range(len(df)))
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
                if _col_name is None:
                    all_df = self.df.empty()
                    self.selected = self.selected.union(self.kept)
                    for i in self.selected:
                        all_df.append(self.df.rows[i])
                    if verbose:
                        if len(all_df) == 0:
                            print(yellow("no row selected"))
                        else:
                            print("{} out of {} row(s) selected".format(len(all_df), len(self.df)))
                    return all_df
                else:
                    col_ind = self.df.head.index(_col_name)
                    for row_ind in self.indices():
                        self.df[row_ind][col_ind] = _value
                    if verbose:
                        if len(self.indices()) == 0:
                            print(yellow("no row updated"))
                        else:
                            print("{} out of {} row(s) updated".format(len(self.indices()), len(self.df)))
                    return self.df

            def indices(self):
                return list(self.selected.union(self.kept))

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

    @property
    def select(self):
        return self.update(col_name=None, new_value=None)
