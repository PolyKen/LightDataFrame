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

    def plot(self, features=['value'], legends=None):
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

        if legends:
            plt.legend(legends)
        else:
            plt.legend(features)

        fig.savefig('figures/' + self.name + "_" + self.date + '.png', format='png')
        plt.close(fig)


class Devices(object):
    @staticmethod
    def get_key_list():
        key_list = ['P_3_2', 'P_3_7', 'P_4_2', 'P_7_5', 'P_7_9', 'P_7_14', 'P_8_7', 'P_12_3', 'P_12_4', 'P_12_5',
                    'P_12_6', 'P_12_7', 'P_12_8', 'P_19_2', 'P_19_5', 'P_19_6', 'P_19_15', 'P_21_8', 'P_21_14',
                    'P_22_3', 'P_22_5', 'P_22_11', 'P_22_12', 'P_23_2', 'P_23_6', 'P_24_9']
        return list(map(lambda x: x + ".Value", key_list))

    @staticmethod
    def get_cooling_list():
        cooling_list = ['P_3_2', 'P_3_7', 'P_7_9', 'P_7_14', 'P_19_2', 'P_19_5', 'P_21_8', 'P_22_11', 'P_22_12',
                        'P_23_2', 'P_23_6']
        return list(map(lambda x: x + ".Value", cooling_list))

    @staticmethod
    def get_damper_list():
        damper_list = ["AHU-R-B1-01", "AHU-R-B2-11", "AHU-R-L3-01", "AHU-R-L3-03", "AHU-R-B2-06", "AHU-R-L4-08",
                       "AHU-R-L4-06", "AHU-R-B2-03", "AHU-R-B2-03", "AHU-R-L4-09", "AHU-R-L4-05", "AHU-R-L3-04",
                       "AHU-R-L4-03", "AHU-R-L4-07"]
        return damper_list


def fetch_elec_power(device_list, limit):
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
        if len(mat) == 0:
            continue
        all_df += EnergyDataFrame.derive(DataFrame.read_matrix(head=head, matrix=mat))

    times = list(map(lambda dt: dt[0:-3], all_df["time"]))
    times = list(set(times))
    print("length of time list: {}".format(len(times)))
    for t in times:
        _df = all_df.select().where("time").contain(t)().print(5)
        value_sum = sum(list(map(float, _df["value"])))
        sum_df.append(["P_total", t, value_sum])
        print(t)
    return sum_df


def fetch_bas_damper(device_list, limit):
    rh = RequestHandler(host="183.6.182.4", port=5000, company="hkl")
    param_list = []
    for device in device_list:
        param = ('get_bas_history', {'prefix': json.dumps(device), 'limit': limit})
        param_list.append(param)

    results = async_run_tasks(rh.async_get, param_list)
    print("all results fetched")

    head = ["time", "value", "name"]
    co2_df = EnergyDataFrame(head=head)
    co2_sp_df = EnergyDataFrame(head=head)
    pos_df = EnergyDataFrame(head=head)
    reg_df = EnergyDataFrame(head=head)
    for res in results:
        d = json.loads(res.text)["result"]
        device = list(d.keys())[0].split("_")[0]
        print(device)
        if device + "_RaCo2" in d:
            co2_mat = d[device + "_RaCo2"]
        elif device + "_RaCO2" in d:
            co2_mat = d[device + "_RaCO2"]
        else:
            continue

        if device + "_RaCo2SP" in d:
            co2_sp_mat = d[device + "_RaCo2SP"]
        elif device + "_RaCO2SP" in d:
            co2_sp_mat = d[device + "_RaCO2SP"]
        else:
            continue

        if device + "_OaDamperPos" in d:
            pos_mat = d[device + "_OaDamperPos"]
        else:
            continue

        if device + "_OaDamperReg" in d:
            reg_mat = d[device + "_OaDamperReg"]
        else:
            continue

        tmp = DataFrame.read_matrix(head=["time", "value"], matrix=co2_mat)
        tmp["name"] = [device for _ in range(len(co2_mat))]
        co2_df += EnergyDataFrame.derive(tmp)

        tmp = DataFrame.read_matrix(head=["time", "value"], matrix=co2_sp_mat)
        tmp["name"] = [device for _ in range(len(co2_sp_mat))]
        co2_sp_df += EnergyDataFrame.derive(tmp)

        tmp = DataFrame.read_matrix(head=["time", "value"], matrix=pos_mat)
        tmp["name"] = [device for _ in range(len(pos_mat))]
        pos_df += EnergyDataFrame.derive(tmp)

        tmp = DataFrame.read_matrix(head=["time", "value"], matrix=reg_mat)
        tmp["name"] = [device for _ in range(len(reg_mat))]
        reg_df += EnergyDataFrame.derive(tmp)

    co2_df.sort("name")
    co2_sp_df.sort("name")
    pos_df.sort("name")
    reg_df.sort("name")

    assert co2_df["name"] == co2_sp_df["name"]
    assert co2_sp_df["name"] == pos_df["name"]
    assert pos_df["name"] == reg_df["name"]

    co2_df["sp_value"] = co2_sp_df["value"]
    co2_df["pos"] = pos_df["value"]
    co2_df["reg"] = reg_df["value"]

    return co2_df


def get_power_values(tpe):
    if tpe == "key":
        _total_df = key()
    elif tpe == "cooling":
        _total_df = cooling()
    else:
        _total_df = fetch_elec_power(tpe, limit=300)
        _total_df.name = tpe

    _total_df.date = now("%m-%d")
    _today_df = _total_df.select().where("time").contain(_total_df.date)().sort("time")
    return _today_df["value"]


if __name__ == "__main__":
    def key():
        _total_df = fetch_elec_power(Devices.get_key_list(), limit=300)
        _total_df.name = "key unit total power"
        return _total_df


    def cooling():
        _total_df = fetch_elec_power(Devices.get_cooling_list(), limit=2672)
        _total_df.name = "cooling unit total power"
        return _total_df


    def yesterday():
        _total_df = fetch_elec_power(Devices.get_key_list(), limit=300)
        _total_df.name = "key unit total power"
        return _total_df


    def damper():
        _total_df = fetch_bas_damper(Devices.get_damper_list(), limit=100)
        _total_df.name = "co2 and damper"
        return _total_df


    total_df = damper()
    date_list = date_list_generator(1, 18, 1, 18, pattern="-")
    device_list = ["AHU-R-B1-01", "AHU-R-B2-11", "AHU-R-L3-01", "AHU-R-L3-03", "AHU-R-B2-06", "AHU-R-L4-08",
                   "AHU-R-L4-06", "AHU-R-B2-03", "AHU-R-B2-03", "AHU-R-L4-09", "AHU-R-L4-05", "AHU-R-L3-04",
                   "AHU-R-L4-03", "AHU-R-L4-07"]

    for date in date_list:
        for device in device_list:
            total_df.date = date
            total_df.name = device
            total_df.select().where("time").contain(date).where("name").contain(device)().sort("time").plot(
                features=["value", "sp_value", "pos", "reg"], legends=["CO2", "CO2 SP", "OaDamperPos", "OaDamperReg"])

    ren = Renderer(output_path="default.html")
    ren.render(path="figures/", image_path_list=list(map(lambda nm: nm + "_" + date_list[0], device_list)), col=2)
