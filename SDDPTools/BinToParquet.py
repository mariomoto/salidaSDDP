from pathlib import Path
from collections import namedtuple
from typing import List, NamedTuple
import psr.factory
import os
import numpy as np
from SDDPTools.SDDPCase import SDDPCase

class Bin2Parquet:

    def __init__(self, sddp_case: SDDPCase):
        self.sddp_case = sddp_case
        self.study = psr.factory.load_study(
            self.sddp_case.pathname
            )

        with open("case_file_agent.csv", "r", encoding="utf-8") as f:
            _ = f.readline()
            dict_cases_files_agents = dict()
            for line in f:
                case, file_name, agents = line.strip().split(",")
                if case not in dict_cases_files_agents:
                    dict_cases_files_agents.update({case:{file_name:agents}})
                else:
                    dict_files_agents = dict_cases_files_agents[case]
                    if file_name not in dict_files_agents:
                        dict_files_agents.update({file_name:agents})

        self.dict_cases_files_agents = dict_cases_files_agents

    def hid_bin_to_parquet(self):

        hydro_plants = self.study.get("HydroPlant")
        dict_hydro_plants = {}
        assert isinstance(hydro_plants, list)
        for plant in hydro_plants:
            generators = plant.get("RefGenerators")
            plant_buses = []
            if isinstance(generators, list):
                for generator in generators:
                    plant_buses.append(generator.get("RefBus"))
                dict_hydro_plants[plant.name.strip()] = [plant.code, plant_buses[0].name.strip(), plant_buses[0].code]

        dict_files_agents = self.dict_cases_files_agents[self.sddp_case.pathname]
        gen_agents = dict_files_agents["gerhid"].split(";")

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None              # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", gen_agents)

        df_f_gen_agents = psr.factory.load_dataframe(
            os.path.join(self.sddp_case.pathname, "gerhid.hdr"), 
            options=load_options)
        df_p_gen_agents = df_f_gen_agents.to_pandas()
        # df_p_gen_agents = df_p_gen_agents.groupby(["year", "month", "hour"]).mean()

        # temp = df_p_gen_agents["MauleB"]
        # df_p_gen_agents["MauleB"] = np.minimum(0, df_p_gen_agents["MauleB"] + df_p_gen_agents["MauleG"])
        # df_p_gen_agents["MauleG"] = np.maximum(0, df_p_gen_agents["MauleG"] + temp)

        df_p_gen_agents.columns = [dict_hydro_plants[name][0] for name in df_p_gen_agents.columns.to_list()]

        df_p_gen_agents.to_parquet(os.path.join(self.sddp_case.pathname, "gerhid.parquet"))

        with open(os.path.join(self.sddp_case.pathname, "hydro_plants.csv"), "w") as f:
            print("genName,genCode,busName,busCode", file=f)
            for gen in gen_agents:
                print(gen, *dict_hydro_plants[gen], sep=',', file=f)

    def add_bus_agents(self):
        buses = self.study.get("Bus")
        assert isinstance(buses, list)
        dict_buses = {bus.name.strip(): bus.code for bus in buses}

        bus_agents = [dict_hydro_plants[gen_agent][1] for gen_agent in gen_agents]
        bus_agents = list(set(bus_agents))
        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None              # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", bus_agents)


        df_f_bus_agents = psr.factory.load_dataframe(
            os.path.join(self.sddp_case.pathname, "cmgbus.hdr"), 
            options=load_options)
        df_p_bus_agents = df_f_bus_agents.to_pandas()
        # df_p_bus_agents = df_p_bus_agents.groupby(["year", "month", "hour"]).mean()
        df_p_bus_agents.columns = [dict_buses[name] for name in df_p_bus_agents.columns.to_list()]
        df_p_bus_agents.to_parquet(
            os.path.join(self.sddp_case.pathname, "cmgbus.parquet"))

        with open(os.path.join(self.sddp_case.pathname, "buses.csv"), "w") as f:
            print("busName,busCode", file=f)
            for bus in bus_agents:
                print(bus, dict_buses[bus], sep=',', file=f)