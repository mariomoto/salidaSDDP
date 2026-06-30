import pandas as pd
import os
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
            raise RuntimeError(
                f"Failed to load dataframe from '{dataframe_pathname}': {e}"
            ) from e

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

        has_hour = "hour" in df_p_agents.index.names
        has_block = "block" in df_p_agents.index.names

        if has_hour:
            days = (df_p_agents.index.get_level_values("hour") - 1) // 24 + 1
            hours = (df_p_agents.index.get_level_values("hour") - 1) % 24

            level_arrays = [
                df_p_agents.index.get_level_values("year"),
                df_p_agents.index.get_level_values("month"),
                days,
                hours,
            ]
            level_names = ["year", "month", "day", "hour"]
        else:
            level_arrays = [
                df_p_agents.index.get_level_values(str(name))
                for name in df_p_agents.index.names
                if name not in ("scenario", "block")
            ]
            level_names = [
                str(name) for name in df_p_agents.index.names
                if name not in ("scenario", "block")
            ]

        if has_block:
            level_arrays.append(df_p_agents.index.get_level_values("block"))
            level_names.append("block")

        if "scenario" in df_p_agents.index.names:
            level_arrays.append(df_p_agents.index.get_level_values("scenario"))
            level_names.append("scenario")

        df_p_agents.index = pd.MultiIndex.from_arrays(level_arrays, names=level_names)

        groupby_levels = level_names.copy()
        for level in self.levels:
            match level:
                case "Y":
                    if "year" in groupby_levels:
                        groupby_levels.remove("year")
                case "M":
                    if "month" in groupby_levels:
                        groupby_levels.remove("month")
                case "D":
                    if "day" in groupby_levels:
                        groupby_levels.remove("day")
                case "H":
                    if "hour" in groupby_levels:
                        groupby_levels.remove("hour")
                case "B":
                    if "block" in groupby_levels:
                        groupby_levels.remove("block")

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
                    if "year" in level_names:
                        df_p_agents["year"] = 1
                        df_p_agents = df_p_agents.set_index("year", append=True)
                case "M":
                    if "month" in level_names:
                        df_p_agents["month"] = 1
                        df_p_agents = df_p_agents.set_index("month", append=True)
                case "D":
                    if "day" in level_names:
                        df_p_agents["day"] = 1
                        df_p_agents = df_p_agents.set_index("day", append=True)
                case "H":
                    if "hour" in level_names:
                        df_p_agents["hour"] = 1
                        df_p_agents = df_p_agents.set_index("hour", append=True)
                case "B":
                    if "block" in level_names:
                        df_p_agents["block"] = 1
                        df_p_agents = df_p_agents.set_index("block", append=True)

        # Reorder index levels to match original level_names order
        final_levels: list[str] = [l for l in level_names if l in df_p_agents.index.names]
        df_p_agents = df_p_agents.reorder_levels(final_levels)

        return df_p_agents
