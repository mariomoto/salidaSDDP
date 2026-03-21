from pathlib import Path
from collections import namedtuple
from typing import List, NamedTuple
import psr.factory
import os
import numpy as np
from SDDPTools.SDDPCommand import SDDPCommand
from SDDPTools.Parameters import DICT_TECH_PLANT

class SDDPParquet:

    def __init__(self, sddp_command: SDDPCommand):
        self.sddp_command = sddp_command
        self.study = psr.factory.load_study(
            self.sddp_command.pathname
            )

        with open("path_tech_agent.csv", "r", encoding="utf-8") as f:
            _ = f.readline()
            dict_paths_techs_agents = dict()
            for line in f:
                path, tech, agents = line.strip().split(",")
                if path not in dict_paths_techs_agents:
                    dict_paths_techs_agents.update({path:{tech:agents}})
                else:
                    dict_techs_agents = dict_paths_techs_agents[path]
                    if tech not in dict_techs_agents:
                        dict_techs_agents.update({tech:agents})

        self.dict_paths_techs_agents = dict_paths_techs_agents

    def ger_bin_to_parquet(self):
        
        pathname = self.sddp_command.pathname
        for tech in self.dict_paths_techs_agents[pathname].keys():
            plant_object = DICT_TECH_PLANT[tech]
            plants = self.study.get(plant_object)
            dict_plants = {}
            assert isinstance(plants, list)
            for plant in plants:
                generators = plant.get("RefGenerators")
                plant_buses = []
                if isinstance(generators, list):
                    for generator in generators:
                        plant_buses.append(generator.get("RefBus"))
                    dict_plants[plant.name.strip()] = [plant.code, plant_buses[0].name.strip(), plant_buses[0].code]

            dict_techs_agents = self.dict_paths_techs_agents[self.sddp_command.pathname]
            agents = dict_techs_agents[tech].split(";")

            load_options = psr.factory.create("DataFrameLoadOptions")
            assert load_options is not None              # if load_options is None, the factory failed to create the object
            load_options.set("FilterAgents", agents)

            df_f_agents = psr.factory.load_dataframe(
                os.path.join(self.sddp_command.pathname, tech + ".hdr"), 
                options=load_options)
            df_p_agents = df_f_agents.to_pandas()
            # df_p_agents = df_p_agents.groupby(["year", "month", "hour"]).mean()

            # temp = df_p_agents["MauleB"]
            # df_p_agents["MauleB"] = np.minimum(0, df_p_agents["MauleB"] + df_p_agents["MauleG"])
            # df_p_agents["MauleG"] = np.maximum(0, df_p_agents["MauleG"] + temp)

            df_p_agents.columns = [dict_plants[name][0] for name in df_p_agents.columns.to_list()]

            df_p_agents.to_parquet(os.path.join(self.sddp_command.pathname, tech + ".parquet"))