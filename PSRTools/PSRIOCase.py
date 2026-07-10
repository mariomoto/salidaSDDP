from collections import defaultdict
from typing import List
import inspect
import pandas as pd
import os
import psr.factory
from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT
from PSRTools.Parameters import LIST_PSRIOOBJECT
from PSRTools.Parameters import PSRIO_COMMANDS
from PSRTools.PSRIOCommand import PSRIOCommand
from utils import my_print, convert_to_short_path


class PSRIOCase:

    def __init__(
        self, output_path: str, psr_study_path: str, original_path: str, psrio_commands_strings: List[str]
    ):
        self.output_path = output_path
        self.pathname = psr_study_path
        self.original_path = original_path
        self.study: psr.factory.Study = psr.factory.load_study(psr_study_path)
        self.psrio_commands: defaultdict[str, List[PSRIOCommand]] = defaultdict(list)

        self.gen_bus_dict = defaultdict(str)
        gen_bus_filepath = os.path.join(self.output_path, "gen_bus.csv")

        with open(gen_bus_filepath, "w", encoding="utf-8") as f:
            f.write("genName,genCode,busName,busCode,tech\n")
            for psrio_object in LIST_PSRIOOBJECT:
                plants = self.study.get(psrio_object)
                assert isinstance(plants, list)
                for plant in plants:
                    bus = self.get_bus(plant)
                    if isinstance(bus, psr.factory.api.DataObject):
                        plant_name = plant.name.strip()
                        if plant_name[3] == ".":
                            tech = plant_name[0:3]
                        else:
                            tech = "HID"
                        f.write(
                            f"{plant_name},{plant.code},{bus.name.strip()},{bus.code},{tech}\n"
                        )
                        self.gen_bus_dict[plant.name.strip()] = bus.name.strip()
        busbar_filepath = os.path.join(self.output_path, "busbar.csv")
        busbars = self.study.get("Bus")
        assert isinstance(busbars, list)
        with open(busbar_filepath, "w", encoding="utf-8") as f:
            f.write("busName,busCode,latitude,longitude\n")
            for bus in busbars:
                lat = bus.get('Latitude')
                lon = bus.get('Longitude')
                if isinstance(lat, list):
                    lat = lat[0] if lat else ""
                if isinstance(lon, list):
                    lon = lon[0] if lon else ""
                f.write(f"{bus.name.strip()},{bus.code},{lat},{lon}\n")

        sddp_filepath = os.path.join(self.output_path, "study.csv")
        with open(sddp_filepath, "w", encoding="utf-8") as f:
            f.write(f"InitialYear, {self.study.get('InitialYear')}\n")
            f.write(f"NumberStages, {self.study.get('NumberStages')}\n")
            f.write(f"NumberSimulations, {self.study.get('NumberSimulations')}\n")

        for string in psrio_commands_strings:
            command, levels, spawn, file, agents = string.split(",")
            if command.lower() in PSRIO_COMMANDS:
                if spawn.strip():
                    spawn_list = [spw.strip() for spw in spawn.strip().split(";")]
                    for spw in spawn_list:
                        match spw:
                            case "C":
                                spawn_file = "cmgbus"
                            case "D":
                                spawn_file = "demxba"
                            case "T":
                                spawn_file = "tarimn"
                            case _:
                                continue
                        spawn_agents = self.get_bus_agents(agents)
                        self.add_psrio_command(
                            psr_study_path,
                            command,
                            levels,
                            "_s",
                            spawn_file,
                            spawn_agents,
                        )

                self.add_psrio_command(
                    psr_study_path, command, levels, "", file, agents
                )

    def add_psrio_command(
        self, psr_study_path, command, levels, spawn, file, agents
    ) -> None:
        psrio_command = PSRIOCommand(
            self.study, psr_study_path, self.original_path, command, levels, spawn, file, agents
        )
        psrio_object_filename = (
            DICT_PSRFILE_PSRIOOBJECT[psrio_command.file].object_filename
            + psrio_command.levels
            + psrio_command.spawn
            + "."
            + psrio_command.command.lower()
        )
        self.psrio_commands[psrio_object_filename].append(psrio_command)

    def get_bus(self, plant) -> psr.factory.DataObject:
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
            current_method = (
                inspect.currentframe().f_code.co_name  # pyright: ignore[reportOptionalMemberAccess]
            )
            current_class = self.__class__.__name__
            prefix = f"{current_class}.{current_method}"
            my_print(f"""
{prefix}: Factory Exception caught: {e}
{prefix}: Pathname: {self.original_path}.
{prefix}: Plant: {plant.name.strip()}.
{prefix}: Continuing without this plant...
            """)
            return None  # type: ignore

    def get_bus_agents(self, agents_string) -> str:
        agents_list = agents_string.split(";")
        bus_agents_list = [self.gen_bus_dict[agent] for agent in agents_list]
        bus_agents_list = list(set(bus_agents_list))
        return ";".join(bus_agents_list)

    def run_psrio_commands(self):
        df_dict = defaultdict(pd.DataFrame)
        for psrio_object_filename, psrio_command_list in self.psrio_commands.items():
            for psrio_command in psrio_command_list:
                try:
                    df_dict[psrio_object_filename] = pd.concat(
                        [
                            df_dict[psrio_object_filename],
                            psrio_command.process_bin_to_dataframe(),
                        ],
                        axis=1,
                    )
                except (RuntimeError, FileNotFoundError, OSError) as e:
                    my_print(
                        f"PSRIOCase.run_psrio_commands: Error processing '{psrio_command.file}': {e}"
                    )
                    continue
        for key, df in df_dict.items():
            filepath = os.path.join(self.output_path, key)
            if os.path.exists(filepath):
                os.remove(filepath)
            try:
                psrio_command = self.psrio_commands[key][0]
                psrio_command.save_dataframe(
                    df.loc[:, ~df.columns.duplicated()], filepath
                )
            except ValueError as e:
                my_print(
                    f"PSRIOCase.run_psrio_commands: Exception caught while saving {filepath}: {e}"
                )


class PSRIOCasesList:
    def __init__(self, output_path: str):

        psrio_commands: defaultdict[str, list[str]] = defaultdict(list)
        original_paths: dict[str, str] = {}
        with open(
            os.path.join(output_path, "psrio_commands.csv"), "r", encoding="latin-1"
        ) as f:
            _ = next(f)
            while line := f.readline().strip():
                line = [item.strip() for item in line.split(",")]
                command, psr_study_path, levels, spawn, file, agents, *_ = line
                if not os.path.isabs(psr_study_path):
                    raise ValueError(
                        f"pathname must be an absolute path, got: {psr_study_path!r}"
                    )
                original_path = psr_study_path
                psr_study_path = convert_to_short_path(psr_study_path)
                psrio_commands_strings = ",".join(
                    [command, levels, spawn, file, agents]
                )
                psrio_commands[psr_study_path].append(psrio_commands_strings)
                original_paths[psr_study_path] = original_path

        self.psrio_cases_list: List[PSRIOCase] = []
        for psr_study_path, psrio_commands_strings in psrio_commands.items():
            original_path = original_paths[psr_study_path]
            my_print(f"PSRIOCasesList: {original_path}.")
            try:
                self.psrio_cases_list.append(
                    PSRIOCase(output_path, psr_study_path, original_path, psrio_commands_strings)
                )
            except psr.factory.api.FactoryException as e:
                error_msg = str(e).replace(psr_study_path, original_path)
                my_print(f"PSRIOCasesList: Skipping '{original_path}': {error_msg}")
                continue

    def get_cases(self) -> List[PSRIOCase]:
        return self.psrio_cases_list
