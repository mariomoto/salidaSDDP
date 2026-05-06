import pandas as pd
import os
import sys
import psr.factory
from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT
class PSRIOCommand():
    def __init__(
            self, study: psr.factory.Study, pathname: str, 
            command: str, levels: str, spawn: str, file: str ,agents: str
        ):
        self.study = study
        self.pathname = pathname
        self.command = command
        self.levels = levels
        self.spawn = spawn
        self.file = file
        self.agents = agents
        self.local_agents = agents
        self.dict_psrio_objects = dict()
    def __repr__(self):
        return f"PSRIOCommand(command={self.command}, levels={self.levels}, spawn={self.spawn}, file={self.file}, agents={self.agents})"
    
    def bin_to_parquet(self) -> pd.DataFrame:

        # keys = {gerter, gerhid, gerbat, gergnd,}
        psrio_object_type = DICT_PSRFILE_PSRIOOBJECT[self.file].object_type
        # if self.file == gerter: palnt_object = "ThermalPlant", etc.
        print(f"bin_to_parquet: {psrio_object_type=}.")
        psrio_objects = self.study.get(psrio_object_type)
        assert isinstance(psrio_objects, list)
        if self.agents.strip() == '':
            # self.local_agents == '' means all agents are selected
            self.local_agents = ";".join([psrio_object.name for psrio_object in psrio_objects])
        for psrio_object in psrio_objects:
            # This goes for every psrio_object within the study. It should be reduced down to the chosen psrio_object (agents).
            psrio_object_name = psrio_object.name.strip()
            if psrio_object_name in self.local_agents:
                self.dict_psrio_objects.update(
                    {
                        psrio_object_name: psrio_object.code
                    }
                )

        load_options = psr.factory.create("DataFrameLoadOptions")
        assert load_options is not None
        # if load_options is None, the factory failed to create the object
        agents_list = self.local_agents.split(";")
        load_options.set("FilterAgents", agents_list)

        dataframe_pathname = os.path.join(self.pathname, self.file + ".hdr")
        try:
            df_f_agents = psr.factory.load_dataframe(
                dataframe_pathname,
                options=load_options,
            )
        except psr.factory.api.FactoryException as e:
            print(f"Factory Exception caught: {e}")
            print(self)
            # Add your custom error handling logic here
            sys.exit()

        df_p_agents = df_f_agents.to_pandas()


        df_p_agents.columns = [
            self.dict_psrio_objects[name]
            for name in df_p_agents.columns.tolist()
        ]

        df_p_agents = self.group_by(df_p_agents)

        factor = DICT_PSRFILE_PSRIOOBJECT[self.file].factor
        df_p_agents /= factor

        return df_p_agents


    def group_by(self, df_p_agents) -> pd.DataFrame:

        days = (df_p_agents.index.get_level_values('hour') - 1) // 24 + 1
        hours = (df_p_agents.index.get_level_values('hour') - 1) % 24

        level_arrays = [
            df_p_agents.index.get_level_values('year'), 
            df_p_agents.index.get_level_values('month'),
            days,
            hours]
        
        level_names = ["year", "month", "day", "hour"]

        if 'scenario' in df_p_agents.index.names:
            level_arrays.append(df_p_agents.index.get_level_values('scenario'))
            level_names.append("scenario")

        df_p_agents.index = pd.MultiIndex.from_arrays(level_arrays, names=level_names)

        for level in self.levels:
            match level:
                case "M": level_names.remove("month")
                case "D": level_names.remove("day")
                case "H": level_names.remove("hour")

        operation = DICT_PSRFILE_PSRIOOBJECT[self.file].operation
        match operation:
            case "mean":
                df_p_agents = df_p_agents.groupby(level_names).mean()
            case "sum":
                df_p_agents = df_p_agents.groupby(level_names).sum()
            case _:
                print(f"Operation '{operation}' not found, falling back to 'sum'.")
                df_p_agents = df_p_agents.groupby(level_names).sum()

        if 'scenario' in level_names and "S" in self.levels:
            level_names.remove("scenario") 
            df_p_agents = df_p_agents.groupby(level_names).mean()

        return df_p_agents
