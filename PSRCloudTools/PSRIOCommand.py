import numpy as np
from typing import List
import psr.factory
import os
from PSRCloudTools.Parameters import DICT_TECH_PLANT, PLANT, PSRIOCommand, PSRIOCommand


class PSRIOCommandsList(List[PSRIOCommand]):
    def __init__(self):
        super().__init__()
        with open(os.path.join("psrio_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline():
                line = [item.strip() for item in line.split(",")]
                command, pathname, tech, agents = line
                self.append(
                    PSRIOCommand(
                        command, pathname, tech, agents
                    )
                )

class PSRIOCase:

    def __init__(self, psrio_command: PSRIOCommand):
        self.psrio_command = psrio_command
        self.study = psr.factory.load_study(self.psrio_command.pathname)
        self.dict_plants = dict()

    def ger_bin_to_parquet(self):

        pathname = self.psrio_command.pathname
        tech = self.psrio_command.tech
        agents = self.psrio_command.agents
        # keys = {gerter, gerhid, gerbat, gergnd}
        plant_object = DICT_TECH_PLANT[tech]
        # if tech == gerter: palnt_object = "ThermalPlant", etc.
        plants = self.study.get(plant_object)
        assert isinstance(plants, list)
        for plant in plants:
            # This goes for every plant within the study. It should be reduced down to the chosen plant (agents).
            if plant.name.strip() in agents:
                bus = self.safe_get_ref_bus(plant)
                self.dict_plants.update(
                    {
                        plant.name.strip(): PLANT(
                            plant_code=plant.code,
                            bus_name=bus.name.strip(),
                            bus_code=bus.code,
                        )
                    }
                )

        df_p_agents = self.get_df_p_agents(tech)
        parquet_pathname = os.path.join(
            self.psrio_command.pathname, tech + ".parquet"
        )
        df_p_agents.to_parquet(parquet_pathname)
        plants_pathname = os.path.join(
            self.psrio_command.pathname, "plants" + ".csv"
        )
        with open(plants_pathname, "w", encoding="utf-8") as f:
            print("genName,genCode,busName,busCode", file=f)
            for gen in self.dict_plants:
                print(gen, *self.dict_plants[gen], sep=',', file=f)

        df_p_bus_agents = self.get_df_p_bus_agents()
        parquet_pathname = os.path.join(
            self.psrio_command.pathname, 'cmgbus' + ".parquet"
        )
        df_p_bus_agents.to_parquet(parquet_pathname)

    def get_df_p_agents(self, tech):

        pathname = self.psrio_command.pathname
        agents = self.psrio_command.agents.split(";")

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", agents)

        dataframe_pathname = os.path.join(pathname, tech + ".hdr")
        df_f_agents = psr.factory.load_dataframe(
            dataframe_pathname,
            options=load_options,
        )
        df_p_agents = df_f_agents.to_pandas()
        # df_p_agents = df_p_agents.groupby(["year", "month", "hour"]).mean()
        if tech == "gerhid":
            temp = df_p_agents["MauleB"]
            df_p_agents["MauleB"] = np.minimum(0, df_p_agents["MauleB"] + df_p_agents["MauleG"])
            df_p_agents["MauleG"] = np.maximum(0, df_p_agents["MauleG"] + temp)

        df_p_agents.columns = [
            self.dict_plants[name].plant_code
            for name in df_p_agents.columns.to_list()
        ]
        return df_p_agents

    def get_df_p_bus_agents(self):
        pathname = self.psrio_command.pathname
        dict_bus_agents = {key.bus_name: key.bus_code for key in self.dict_plants.values()}
        bus_agents = list({key.bus_name for key in self.dict_plants.values()})
        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", bus_agents)

        dataframe_pathname = os.path.join(self.psrio_command.pathname, 'cmgbus' + ".hdr")
        df_f_bus_agents = psr.factory.load_dataframe(
            dataframe_pathname,
            options=load_options,
        )
        df_p_bus_agents = df_f_bus_agents.to_pandas()
        df_p_bus_agents.columns = [
            dict_bus_agents[name]
            for name in df_p_bus_agents.columns.to_list()
        ]
        return df_p_bus_agents
    
    def safe_get_ref_bus(self, plant) -> psr.factory.DataObject:
        """Safely get RefBus from plant, trying generators first then direct."""
        # Try generators pathname
        try:
            generators = plant.get("RefGenerators")
            if isinstance(generators, list) and generators:
                generator = generators[0]
                return generator.get("RefBus")
        except Exception:
            pass

        # Fallback to direct RefBus
        try:
            return plant.get("RefBus")
        except Exception as e:
            print(f"Failed to get RefBus: {e}")
            return None  # type: ignore
