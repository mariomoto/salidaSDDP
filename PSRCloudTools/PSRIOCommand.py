import numpy as np
import pandas as pd
from typing import List
from pathlib import Path
import psr.factory
import glob
import os
from PSRCloudTools.Parameters import DICT_FILE_PSRIOOBJECT, PSRIOCommand, PSRIOCommand


class PSRIOCommandsList(List[PSRIOCommand]):
    def __init__(self):
        super().__init__()
        with open(os.path.join("psrio_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline():
                line = [item.strip() for item in line.split(",")]
                command, pathname, flats, file, agents = line
                self.append(
                    PSRIOCommand(
                        command, pathname, flats, file, agents
                    )
                )

        folders = {item.pathname for item in self}
        for item in self:
            command = item.command
            if command == "Parquet":
                folder = item.pathname
                object_filename = DICT_FILE_PSRIOOBJECT[item.file].object_filename
                flats = item.flats
                files_in_dir = glob.iglob(os.path.join(folder, object_filename  + flats + ".parquet"))
                for _file in files_in_dir:
                    os.remove(_file)

class PSRIOCase:

    def __init__(self, psrio_command: PSRIOCommand):
        self.psrio_command = psrio_command
        self.study = psr.factory.load_study(self.psrio_command.pathname)
        self.dict_psrio_objects = dict()


    def bin_to_parquet(self):

        pathname = self.psrio_command.pathname
        file = self.psrio_command.file
        agents = self.psrio_command.agents
        self.dict_psrio_objects = dict()
        # keys = {gerter, gerhid, gerbat, gergnd}
        psrio_object_type = DICT_FILE_PSRIOOBJECT[file].object_type
        # if file == gerter: palnt_object = "ThermalPlant", etc.
        psrio_objects = self.study.get(psrio_object_type)
        assert isinstance(psrio_objects, list)
        if (agents := self.psrio_command.agents.strip()) == '':
            # agents == '' means all agents are selected
            agents = ";".join([psrio_object.name for psrio_object in psrio_objects])
        for psrio_object in psrio_objects:
            # This goes for every psrio_object within the study. It should be reduced down to the chosen psrio_object (agents).
            psrio_object_name = psrio_object.name.strip()
            if psrio_object_name in agents:
                self.dict_psrio_objects.update(
                    {
                        psrio_object_name: psrio_object.code
                    }
                )

        parquet_filename = DICT_FILE_PSRIOOBJECT[file].object_filename
        df_p_agents = self.get_df_p_agents(file, agents)

        parquet_pathname = os.path.join(
            pathname, parquet_filename + self.psrio_command.flats + ".parquet"
        )
        if not os.path.exists(parquet_pathname):
            df_p_agents.to_parquet(parquet_pathname)
        else:
            df_p_prev = pd.read_parquet(parquet_pathname)
            df_p_agents = pd.concat([df_p_prev, df_p_agents], axis=1)
            df_p_agents.to_parquet(parquet_pathname)

    def get_df_p_agents(self, file, agents):

        pathname = self.psrio_command.pathname

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        agents_list = agents.split(";")
        load_options.set("FilterAgents", agents_list)

        dataframe_pathname = os.path.join(pathname, file + ".hdr")
        df_f_agents = psr.factory.load_dataframe(
            dataframe_pathname,
            options=load_options,
        )
        df_p_agents = df_f_agents.to_pandas()
        days = (df_p_agents.index.get_level_values('hour') - 1) // 24 + 1
        hours = (df_p_agents.index.get_level_values('hour') - 1) % 24

        df_p_agents.index = pd.MultiIndex.from_arrays([
            df_p_agents.index.get_level_values('scenario'),
            df_p_agents.index.get_level_values('year'),
            df_p_agents.index.get_level_values('month'),
            days,
            hours
        ], names=["scenario", "year", "month", "day", "hour"])

        flats = ["scenario", "year", "month", "day", "hour"]
        for flat in self.psrio_command.flats:
            match flat:
                case "S": flats.remove("scenario")
                case "M": flats.remove("month")
                case "D": flats.remove("day")
                case "H": flats.remove("hour")

        df_p_agents = df_p_agents.groupby(flats).sum()

        df_p_agents.columns = [
            self.dict_psrio_objects[name]
            for name in df_p_agents.columns.tolist()
        ]
        return df_p_agents

