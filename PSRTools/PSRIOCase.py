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

    def __init__(self, output_folder: str, pathname: str, psrio_commands_strings: List[str]):
        self.output_folder = output_folder
        self.pathname = pathname
        self.study: psr.factory.Study = psr.factory.load_study(pathname)
        self.psrio_commands: defaultdict[str, List[PSRIOCommand]] = defaultdict(list)

        self.gen_bus_dict = defaultdict(str)
        gen_bus_filepath = os.path.join(self.output_folder, "gen_bus.csv")

        with open(gen_bus_filepath, "w", encoding="utf-8") as f:
            f.write("genName,genCode,busName,busCode,tech\n")
            for psrio_object in LIST_PSRIOOBJECT:
                plants = self.study.get(psrio_object)
                assert isinstance(plants, list)
                for plant in plants:
                    bus = self.get_bus(plant)
                    if isinstance(bus, psr.factory.api.DataObject):
                        plant_name = plant.name.strip()
                        if plant_name[3] == '.':
                            tech = plant_name[0:3]
                        else:
                            tech = 'HID'
                        f.write(
                            f"{plant_name},{plant.code},{bus.name.strip()},{bus.code},{tech}\n"
                        )
                        self.gen_bus_dict[plant.name.strip()] = bus.name.strip()
        sddp_filepath = os.path.join(self.output_folder, "study.csv")
        with open(sddp_filepath, "w", encoding="utf-8") as f:
            f.write(f"InitialYear, {self.study.get('InitialYear')}\n")
            f.write(f"NumberStages, {self.study.get('NumberStages')}\n")
            f.write(f"NumberSimulations, {self.study.get('NumberSimulations')}\n")

        for string in psrio_commands_strings:
            command, levels, spawn, file, agents = string.split(",")
            if command in PSRIO_COMMANDS:
                if spawn.strip():
                    spawn_list = [spw.strip() for spw in spawn.strip().split(";")]
                    for spw in spawn_list:
                        if spw == "D":
                            spawn_file = "demxba"
                        else:
                            spawn_file = "cmgbus"
                        spawn_agents = self.get_bus_agents(agents)
                        self.add_psrio_command(
                            pathname, command, levels, "_s", spawn_file, spawn_agents
                        )

                self.add_psrio_command(
                    pathname, command, levels, "", file, agents
                )

    def add_psrio_command(self, pathname, command, levels, spawn, file, agents) -> None:
        psrio_command = PSRIOCommand(
            self.study, pathname, command, levels, spawn, file, agents
        )
        psrio_object_filename = (
            DICT_PSRFILE_PSRIOOBJECT[psrio_command.file].object_filename
            + levels
            + spawn
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
            current_method = inspect.currentframe().f_code.co_name # pyright: ignore[reportOptionalMemberAccess]
            current_class = self.__class__.__name__
            prefix = f"{current_class}.{current_method}"
            my_print(f"""
{prefix}: Factory Exception caught: {e}
{prefix}: Pathname: {self.pathname}.
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
                df_dict[psrio_object_filename] = pd.concat(
                    [df_dict[psrio_object_filename], psrio_command.bin_to_parquet()], 
                    axis=1
                )
        for key, df in df_dict.items():
            parquet_pathname = os.path.join(
                self.output_folder, key + ".parquet"
            )
            if os.path.exists(parquet_pathname):
                os.remove(parquet_pathname)
            df.to_parquet(parquet_pathname)


class PSRIOCasesList:
    def __init__(self, output_folder: str):

        psrio_commands: defaultdict[str, list[str]] = defaultdict(list)
        with open(os.path.join(output_folder, "psrio_commands.csv"), "r", encoding="latin-1") as f:
            _ = next(f)
            while line := f.readline().strip():
                line = [item.strip() for item in line.split(",")]
                command, pathname, levels, spawn, file, agents = line
                if not os.path.isabs(pathname):
                    raise ValueError(f"pathname must be an absolute path, got: {pathname!r}")
                pathname = convert_to_short_path(pathname)
                psrio_commands_strings = ",".join(
                    [command, levels, spawn, file, agents]
                )
                psrio_commands[pathname].append(psrio_commands_strings)

        self.psrio_cases_list: List[PSRIOCase] = []
        for pathname, psrio_commands_strings in psrio_commands.items():
            my_print(f"PSRIOCasesList: {pathname}.")
            self.psrio_cases_list.append(PSRIOCase(output_folder, pathname, psrio_commands_strings))

    def get_cases(self) -> List[PSRIOCase]:
        return self.psrio_cases_list
