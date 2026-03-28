import numpy as np
from typing import List
import psr.factory
import os
from PSRCloudTools.Parameters import DICT_FILE_PSRIOOBJECT, PSRIOCommand, PSRIOCommand


class PSRIOCommandsList(List[PSRIOCommand]):
    def __init__(self):
        super().__init__()
        with open(os.path.join("psrio_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline():
                line = [item.strip() for item in line.split(",")]
                command, pathname, file, agents = line
                self.append(
                    PSRIOCommand(
                        command, pathname, file, agents
                    )
                )

class PSRIOCase:

    def __init__(self, psrio_command: PSRIOCommand):
        self.psrio_command = psrio_command
        self.study = psr.factory.load_study(self.psrio_command.pathname)
        self.dict_psrio_objects = dict()

    def bin_to_parquet(self):

        pathname = self.psrio_command.pathname
        file = self.psrio_command.file
        agents = self.psrio_command.agents
        # keys = {gerter, gerhid, gerbat, gergnd}
        psrio_object = DICT_FILE_PSRIOOBJECT[file]
        # if file == gerter: palnt_object = "ThermalPlant", etc.
        psrio_objects = self.study.get(psrio_object)
        assert isinstance(psrio_objects, list)
        for psrio_object in psrio_objects:
            # This goes for every psrio_object within the study. It should be reduced down to the chosen psrio_object (agents).
            psrio_object_name = psrio_object.name.strip()
            if psrio_object_name in agents:
                self.dict_psrio_objects.update(
                    {
                        psrio_object_name: psrio_object.code
                    }
                )

        df_p_agents = self.get_df_p_agents(file)
        parquet_pathname = os.path.join(
            pathname, file + ".parquet"
        )
        df_p_agents.to_parquet(parquet_pathname)

    def get_df_p_agents(self, file):

        pathname = self.psrio_command.pathname
        agents = self.psrio_command.agents.split(";")

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        load_options.set("FilterAgents", agents)

        dataframe_pathname = os.path.join(pathname, file + ".hdr")
        df_f_agents = psr.factory.load_dataframe(
            dataframe_pathname,
            options=load_options,
        )
        df_p_agents = df_f_agents.to_pandas()
        # df_p_agents = df_p_agents.groupby(["year", "month", "hour"]).mean()

        df_p_agents.columns = [
            self.dict_psrio_objects[name]
            for name in self.dict_psrio_objects
        ]
        return df_p_agents

