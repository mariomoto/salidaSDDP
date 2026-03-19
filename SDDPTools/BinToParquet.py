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

    def hid_bin_to_parquet(self):
        study = psr.factory.load_study(
            self.sddp_case.pathname
            )

        hydro_plants = study.get("HydroPlant")
        dict_hydro_plants = {}
        assert isinstance(hydro_plants, list)
        for plant in hydro_plants:
            generators = plant.get("RefGenerators")
            plant_buses = []
            if isinstance(generators, list):
                for generator in generators:
                    plant_buses.append(generator.get("RefBus"))
                dict_hydro_plants[plant.name.strip()] = [plant.code, plant_buses[0].name.strip(), plant_buses[0].code]

        gen_agents = [
            # "BAT.CELDASOL",
            # "BAT.DAS",
            # "BAT.DAS_2_ED",
            # "CAR.SMARIA",
            # "DIE.CANDEL_1",
            # "DIE.CANDEL_2",
            # "DIE.LPINOS",
            # "DIE.NEHUENC3",
            # "EOL.HORIZONN",
            # "EOL.HORIZONS",
            # "GNL.Nehu2GAR",
            # "GNL.Nehuenc1"
            # "SOL.DAS",
            "Angostura",
            "Blanco",
            "Canutillar",
            "Chacabuquito",
            "Chiburgo",
            "Colbun",
            "Hornito",
            "Juncal",
            "La_Mina",
            "LosQuilos",
            "Machicura",
            "MauleB",
            "MauleG",
            "Quilleco",
            "Rucue",
            "SanClemente",
            "SanIgnacio"
        ]

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None              # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", gen_agents)

        df_f_gen_agents = psr.factory.load_dataframe(
            os.path.join(self.sddp_case.pathname, "gerhid.hdr"), options=load_options)
        df_p_gen_agents = df_f_gen_agents.to_pandas()
        # df_p_gen_agents = df_p_gen_agents.groupby(["year", "month", "hour"]).mean()

        temp = df_p_gen_agents["MauleB"]
        df_p_gen_agents["MauleB"] = np.minimum(0, df_p_gen_agents["MauleB"] + df_p_gen_agents["MauleG"])
        df_p_gen_agents["MauleG"] = np.maximum(0, df_p_gen_agents["MauleG"] + temp)

        df_p_gen_agents.columns = [dict_hydro_plants[name][0] for name in df_p_gen_agents.columns.to_list()]

        df_p_gen_agents.to_parquet(os.path.join(self.sddp_case.pathname, "gerhid.parquet"))

        with open(os.path.join(self.sddp_case.pathname, "hydro_plants.csv"), "w") as f:
            print("genName,genCode,busName,busCode", file=f)
            for gen in gen_agents:
                print(gen, *dict_hydro_plants[gen], sep=',', file=f)