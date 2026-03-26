import numpy as np
import psr.factory
import os
from SDDPTools.Parameters import DICT_TECH_PLANT, PLANT, SDDPCloudCommand


class SDDPParquet:

    def __init__(self, sddp_cloud_command: SDDPCloudCommand):
        self.sddp_cloud_command = sddp_cloud_command
        self.study = psr.factory.load_study(self.sddp_cloud_command.pathname)
        self.dict_paths_plants = dict()
        dict_paths_techs_agents = dict()
        with open("path_tech_agent.csv", "r", encoding="utf-8") as f:
            _ = f.readline()
            for line in f:
                path, tech, agents = line.strip().split(",")
                if path not in dict_paths_techs_agents:
                    dict_paths_techs_agents.update({path: {tech: agents}})
                else:
                    dict_techs_agents = dict_paths_techs_agents[path]
                    if tech not in dict_techs_agents:
                        dict_techs_agents.update({tech: agents})

        self.dict_paths_techs_agents = dict_paths_techs_agents

    def ger_bin_to_parquet(self):

        pathname = self.sddp_cloud_command.pathname
        if pathname not in self.dict_paths_plants:
            self.dict_paths_plants.update({pathname: dict()})
        for tech in self.dict_paths_techs_agents[pathname].keys():
            # keys = {gerter, gerhid, gerbat, gergnd}
            plant_object = DICT_TECH_PLANT[tech]
            # if tech == gerter: palnt_object = "ThermalPlant", etc.
            plants = self.study.get(plant_object)
            assert isinstance(plants, list)
            for plant in plants:
                # This goes for every plant within the study. It should be reduced down to the chosen plant (agents).
                if plant.name.strip() in self.dict_paths_techs_agents[pathname][
                    tech
                ].split(";"):
                    bus = self.safe_get_ref_bus(plant)
                    self.dict_paths_plants[pathname].update(
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
                self.sddp_cloud_command.pathname, tech + ".parquet"
            )
            df_p_agents.to_parquet(parquet_pathname)
        plants_pathname = os.path.join(
            self.sddp_cloud_command.pathname, "plants" + ".csv"
        )
        with open(plants_pathname, "w", encoding="utf-8") as f:
            print("genName,genCode,busName,busCode", file=f)
            for gen in self.dict_paths_plants[pathname]:
                print(gen, *self.dict_paths_plants[pathname][gen], sep=',', file=f)

        df_p_bus_agents = self.get_df_p_bus_agents()
        parquet_pathname = os.path.join(
            self.sddp_cloud_command.pathname, 'cmgbus' + ".parquet"
        )
        df_p_bus_agents.to_parquet(parquet_pathname)

    def get_df_p_agents(self, tech):

        pathname = self.sddp_cloud_command.pathname
        dict_techs_agents = self.dict_paths_techs_agents[pathname]
        agents = dict_techs_agents[tech].split(";")

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", agents)

        dataframe_pathname = os.path.join(self.sddp_cloud_command.pathname, tech + ".hdr")
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
            self.dict_paths_plants[pathname][name].plant_code
            for name in df_p_agents.columns.to_list()
        ]
        return df_p_agents

    def get_df_p_bus_agents(self):
        pathname = self.sddp_cloud_command.pathname
        dict_bus_agents = {key.bus_name: key.bus_code for key in self.dict_paths_plants[pathname].values()}
        bus_agents = list({key.bus_name for key in self.dict_paths_plants[pathname].values()})
        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", bus_agents)

        dataframe_pathname = os.path.join(self.sddp_cloud_command.pathname, 'cmgbus' + ".hdr")
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
        # Try generators path
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
