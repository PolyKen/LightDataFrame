from DataFrame import DataFrame
import matplotlib.pyplot as plt
from utils import time2float, RequestHandler, async_run_tasks, Renderer, date_list_generator, now
import json


class EnergyDataFrame(DataFrame):
    def __init__(self, **kwargs):
        super(EnergyDataFrame, self).__init__(**kwargs)

    @staticmethod
    def derive(base_data_frame):
        assert type(base_data_frame) == DataFrame
        return EnergyDataFrame(head=base_data_frame.head, rows=base_data_frame.rows, name=base_data_frame.name,
                               date=base_data_frame.date)

    def plot(self, features=['value']):
        fig = plt.figure()
        x = list(self['time'])
        x = list(map(lambda dt: dt.split()[1], x))
        x = list(map(time2float, x))

        for feat in features:
            y = list(self[feat])
            plt.plot(x, y)

        title = ""
        if self.name:
            title += self.name
        if self.date:
            if self.name:
                title += "   "
            title += self.date
        if len(title) > 0:
            plt.title(title)

        fig.savefig('figures/' + self.name + "_" + self.date + '.png', format='png')
        plt.close(fig)


def fetch(device_list, limit):
    rh = RequestHandler(host="183.6.182.4", port=5000, company="hkl")
    param_list = []
    for device in device_list:
        param = ('get_elec_history', {'name': json.dumps(device), 'limit': limit})
        param_list.append(param)

    results = async_run_tasks(rh.async_get, param_list)
    print("all results fetched")

    head = ["name", "time", "value"]
    all_df = EnergyDataFrame(head=head)
    sum_df = all_df.empty()
    for res in results:
        mat = json.loads(res.text)["result"]
        all_df += EnergyDataFrame.derive(DataFrame.read_matrix(head=head, matrix=mat))

    times = list(map(lambda dt: dt[0:-3], all_df["time"]))
    times = list(set(times))
    print(len(times))
    for t in times:
        _df = all_df.select().where("time").contain(t)().print(5)
        value_sum = sum(list(map(float, _df["value"])))
        sum_df.append(["P_total", t, value_sum])
        print(t)
    return sum_df


if __name__ == "__main__":
    def key():
        key_list = ['P_3_2', 'P_3_7', 'P_4_2', 'P_7_5', 'P_7_9', 'P_7_14', 'P_8_7', 'P_12_3', 'P_12_4', 'P_12_5',
                    'P_12_6', 'P_12_7', 'P_12_8', 'P_19_2', 'P_19_5', 'P_19_6', 'P_19_15', 'P_21_8', 'P_21_14',
                    'P_22_3', 'P_22_5', 'P_22_11', 'P_22_12', 'P_23_2', 'P_23_6', 'P_24_9']
        key_list = list(map(lambda x: x + ".Value", key_list))

        _total_df = fetch(key_list, limit=300)
        _total_df.save_csv("key.csv")

        _total_df.name = "key unit total power"
        return _total_df


    def cooling():
        cooling_list = ['P_3_2', 'P_3_7', 'P_7_9', 'P_7_14', 'P_19_2', 'P_19_5', 'P_21_8', 'P_22_11', 'P_22_12',
                        'P_23_2', 'P_23_6']
        cooling_list = list(map(lambda x: x + ".Value", cooling_list))

        _total_df = fetch(cooling_list, limit=2672)
        _total_df.save_csv("cooling.csv")
        # total_df = EnergyDataFrame.derive(DataFrame.read_csv("cooling.csv"))
        # total_df["value"] = list(map(float, total_df["value"]))

        _total_df.name = "cooling unit total power"
        return _total_df

    def yesterday():
        key_list = ['P_3_2', 'P_3_7', 'P_4_2', 'P_7_5', 'P_7_9', 'P_7_14', 'P_8_7', 'P_12_3', 'P_12_4', 'P_12_5',
                    'P_12_6', 'P_12_7', 'P_12_8', 'P_19_2', 'P_19_5', 'P_19_6', 'P_19_15', 'P_21_8', 'P_21_14',
                    'P_22_3', 'P_22_5', 'P_22_11', 'P_22_12', 'P_23_2', 'P_23_6', 'P_24_9']
        key_list = list(map(lambda x: x + ".Value", key_list))

        _total_df = fetch(key_list, limit=300)

        _total_df.name = "key unit total power"
        return _total_df

    def test():
        _total_df = EnergyDataFrame.derive(DataFrame.read_csv("cooling.csv"))
        _total_df["value"] = list(map(float, _total_df["value"]))


    def get_power_values(tpe):
        if tpe == "key":
            _total_df = key()
        elif tpe == "cooling":
            _total_df = cooling()
        else:
            _total_df = fetch(tpe, limit=300)
            _total_df.name = tpe

        _total_df.date = now("%m-%d")
        _today_df = _total_df.select().where("time").contain(_total_df.date)().sort("time")
        return _today_df["value"]

    total_df = key()
    # total_df = cooling()
    # total_df = yesterday()
    #
    # # date_list = date_list_generator(1, 3, 1, 15, pattern="-")
    date_list = ["01-16"]
    #
    for date in date_list:
        total_df.date = date
        total_df.select().where("time").contain(date)().sort("time").plot(features=["value"])

    ren = Renderer(output_path="yesterday.html")
    ren.render(path="figures/", image_path_list=list(map(lambda dt: total_df.name + "_" + dt, date_list)), col=2)
