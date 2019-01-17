from functools import reduce
import requests
import asyncio
import time
import os
import datetime


color_dict = {"red": "31", "green": "32",
              "yellow": "33", "blue": "34", "purple": "35"}
month_size_dict = {"1": 31, "2": 28, "3": 31, "4": 30, "5": 31, "6": 30, "7": 31, "8": 31, "9": 30, "10": 31, "11": 30,
                   "12": 31}


def join(string, pattern=","):
    return reduce(lambda x, y: "{}{}{}".format(x, pattern, y), string)


def now(fmt):
    return datetime.datetime.now().strftime(fmt)


def time2float(time_str):
    try:
        hour, minute, second = time_str.split(':')
    except ValueError:
        try:
            hour, minute = time_str.split(':')
            second = 0
        except ValueError:
            try:
                hour = time_str
                minute, second = 0, 0
            except ValueError:
                print("time str = {}".format(time_str))
                raise ValueError
    return float(hour) + float(minute) / 60 + float(second) / 3600


def date_list_generator(start_month, start_day, end_month, end_day, pattern="/"):
    assert (start_day <= month_size_dict[str(start_month)])
    assert (end_day <= month_size_dict[str(end_month)])

    date_list = []
    for m in range(start_month, end_month + 1):
        size = month_size_dict[str(m)] if m < end_month else end_day
        start = start_day if m == start_month else 1
        for d in range(start, size + 1):
            month_str = str(m) if m >= 10 else "0" + str(m)
            day_str = str(d) if d >= 10 else "0" + str(d)
            date_list.append(month_str + pattern + day_str)

    return date_list


def colorize(string, color):
    colored = "\033[" + color_dict[color] + "m" + string + "\033[0m"
    return colored


def red(string):
    return colorize(string, "red")


def yellow(string):
    return colorize(string, "yellow")


def green(string):
    return colorize(string, "green")


def async_run_tasks(coro_func_list, para_list):
    loop = asyncio.get_event_loop()
    tasks = []
    if type(coro_func_list) is not list:
        coro_func_list = [coro_func_list] * len(para_list)
    for i, func in enumerate(coro_func_list):
        para = para_list[i]
        coro = func(*para)
        task = loop.create_task(coro)
        tasks.append(task)
    print("> Running {} tasks:".format(len(tasks)))
    loop.run_until_complete(asyncio.wait(tasks))
    tasks_results = [task.result() for task in tasks]
    return tasks_results


def retry(interval=10, repeat_times=3):
    def wrapper2(func):
        def wrapper(*args, **kwargs):
            repeat = repeat_times
            while repeat != 0:
                try:
                    res = func(*args, **kwargs)
                    return res
                except Exception as e:
                    print(red("Exception occured: " + str(e)))
                    print(red("try again"))
                    time.sleep(interval)
                    repeat -= 1
            raise ConnectionError

        wrapper.__name__ = func.__name__
        return wrapper

    return wrapper2


class RequestHandler(object):
    def __init__(self, host, port, company):
        self.url = 'http://{}:{}/?/{}'.format(host, port, company).replace('?', '{}')

    def get(self, service, param):
        return requests.get(self.url.format(service), param)

    @retry(interval=1, repeat_times=3)
    async def async_get(self, service, params):
        loop = asyncio.get_event_loop()
        url = self.url.format(service)
        r = await loop.run_in_executor(None, lambda p: requests.get(url, p), params)
        if r.status_code == 200:
            return r


class Renderer(object):
    def __init__(self, output_path):
        self.output_path = output_path

    @staticmethod
    def image(img_path):
        return '<td><img src="' + img_path + '" alt="No Data"></td>'

    @staticmethod
    def row(cell_lst):
        ret = "<tr>"
        for cell in cell_lst:
            ret += cell
        ret += "</tr>\n"
        return ret

    @staticmethod
    def table(row_lst):
        tbl = ""
        for r in row_lst:
            tbl += r
        return tbl

    def render(self, path, render_lst, col, title_info=""):
        head = "<!DOCTYPE html><html><body>" + \
               "<h2>" + title_info + "</h2>" + \
               '<table style="width:100%">'
        foot = "</table></body></html>"

        path_lst = [os.path.join(path, e + '.png') for e in render_lst]
        print("render list: {}".format(render_lst))

        row_lst = []
        idx = 0
        while idx < len(render_lst):
            cell_lst = []
            for i in range(col):
                if idx >= len(render_lst):
                    break
                cell_lst.append(self.image(path_lst[idx]))
                idx += 1
            row_lst.append(self.row(cell_lst))
        body = self.table(row_lst)

        with open(self.output_path, 'w') as the_file:
            the_file.write(head + body + foot)
