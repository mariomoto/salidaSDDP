import pandas as pd
import os
import sys
import psr.factory
from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT
from utils import my_print


class PSRIOCommand:
    def __init__(
        self,
        study: psr.factory.Study,
        pathname: str,
        command: str,
        levels: str,
        spawn: str,
        file: str,
        agents: str,
    ):
        self.study = study
        self.pathname = pathname
        self.command = command
        self.levels = levels if levels else "X"
        self.spawn = spawn
        self.file = file
        self.agents = agents
        self.local_agents = agents
        self.dict_psrio_objects = dict()

    def __repr__(self):
        return f"PSRIOCommand(command={self.command}, levels={self.levels}, spawn={self.spawn}, file={self.file}, agents={self.agents})"

    def process_bin_to_dataframe(self) -> pd.DataFrame:

        # keys = {gerter, gerhid, gerbat, gergnd,}
        psrio_object_type = DICT_PSRFILE_PSRIOOBJECT[self.file].object_type
        # if self.file == gerter: palnt_object = "ThermalPlant", etc.
        my_print(f"bin_to_parquet: {psrio_object_type=}.")
        psrio_objects = self.study.get(psrio_object_type)
        assert isinstance(psrio_objects, list)
        if self.agents.strip() == "":
            # self.local_agents == '' means all agents are selected
            self.local_agents = ";".join(
                [psrio_object.name for psrio_object in psrio_objects]
            )
        for psrio_object in psrio_objects:
            # This goes for every psrio_object within the study. It should be reduced down to the chosen psrio_object (agents).
            psrio_object_name = psrio_object.name.strip()
            if psrio_object_name in self.local_agents:
                self.dict_psrio_objects.update({psrio_object_name: psrio_object.code})

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
            my_print(f"PSRIOCommand.bin_to_parquet: {e}")
            print(self)
            # Add your custom error handling logic here
            sys.exit()

        df_p_agents = df_f_agents.to_pandas()

        df_p_agents.columns = [
            self.dict_psrio_objects[name] for name in df_p_agents.columns.tolist()
        ]

        df_p_agents = self.group_by(df_p_agents)

        factor = DICT_PSRFILE_PSRIOOBJECT[self.file].factor
        df_p_agents /= factor

        return df_p_agents

    def save_dataframe(self, df: pd.DataFrame, filepath: str) -> None:
        match self.command.lower():
            case "parquet":
                df.to_parquet(filepath)
            case "csv":
                df.to_csv(filepath)
            case _:
                raise ValueError(f"Unsupported format: {self.command!r}")

    def group_by(self, df_p_agents: pd.DataFrame) -> pd.DataFrame:

        days = (df_p_agents.index.get_level_values("hour") - 1) // 24 + 1
        hours = (df_p_agents.index.get_level_values("hour") - 1) % 24

        level_arrays = [
            df_p_agents.index.get_level_values("year"),
            df_p_agents.index.get_level_values("month"),
            days,
            hours,
        ]

        level_names = ["year", "month", "day", "hour"]

        if "scenario" in df_p_agents.index.names:
            level_arrays.append(df_p_agents.index.get_level_values("scenario"))
            level_names.append("scenario")

        df_p_agents.index = pd.MultiIndex.from_arrays(level_arrays, names=level_names)

        groupby_levels = level_names.copy()
        for level in self.levels:
            match level:
                case "Y":
                    groupby_levels.remove("year")
                case "M":
                    groupby_levels.remove("month")
                case "D":
                    groupby_levels.remove("day")
                case "H":
                    groupby_levels.remove("hour")

        operation = DICT_PSRFILE_PSRIOOBJECT[self.file].operation
        match operation:
            case "mean":
                df_p_agents = df_p_agents.groupby(groupby_levels).mean()
            case "sum":
                df_p_agents = df_p_agents.groupby(groupby_levels).sum()
            case _:
                my_print(f"Operation '{operation}' not found, falling back to 'sum'.")
                df_p_agents = df_p_agents.groupby(groupby_levels).sum()

        if "scenario" in groupby_levels and "S" in self.levels:
            groupby_levels.remove("scenario")
            df_p_agents = df_p_agents.groupby(groupby_levels).mean()

        # Re-insert grouped-away levels as constant values
        for level in self.levels:
            match level:
                case "Y":
                    df_p_agents["year"] = 1
                    df_p_agents = df_p_agents.set_index("year", append=True)
                case "M":
                    df_p_agents["month"] = 1
                    df_p_agents = df_p_agents.set_index("month", append=True)
                case "D":
                    df_p_agents["day"] = 1
                    df_p_agents = df_p_agents.set_index("day", append=True)
                case "H":
                    df_p_agents["hour"] = 1
                    df_p_agents = df_p_agents.set_index("hour", append=True)

        # Reorder index levels to match original level_names order
        final_levels = [l for l in level_names if l in df_p_agents.index.names]
        df_p_agents = df_p_agents.reorder_levels(final_levels)

        return df_p_agents
