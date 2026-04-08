from collections import defaultdict
from typing import List
import pandas as pd
import os
import psr.factory
from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT
from PSRTools.Parameters import DICT_PSRPLANTCSV_PSRIOOBJECT
from PSRTools.Parameters import LIST_PSRIOOBJECT
from PSRTools.PSRIOCommand import PSRIOCommand


class PSRIOCase:

    def __init__(self, pathname: str, psrio_commands_strings: List[str]):
        self.pathname = pathname
        self.study: psr.factory.Study = psr.factory.load_study(pathname)
        self.psrio_commands: defaultdict[str, List[PSRIOCommand]] = defaultdict(list)

        self.gen_bus_dict = defaultdict(str)
        gen_bus_filepath = os.path.join(self.pathname, "gen_bus.csv")

        with open(gen_bus_filepath, "w", encoding="utf-8") as f:
            f.write("genName,genCode,busName,busCode\n")
            for psrio_object in LIST_PSRIOOBJECT:
                plants = self.study.get(psrio_object)
                assert isinstance(plants, list)
                for plant in plants:
                    bus = self.get_bus(plant)
                    f.write(
                        f"{plant.name.strip()}, {plant.code}, {bus.name.strip()}, {bus.code}\n"
                    )
                    self.gen_bus_dict[plant.name.strip()] = bus.name.strip()

        for string in psrio_commands_strings:
            command, levels, spawn, file, agents = string.split(",")

            if spawn.strip():
                spawn_list = [spw.strip() for spw in spawn.strip().split(";")]
                for spw in spawn_list:
                    if spw == "D":
                        spawn_file = "demxba"
                    else:
                        spawn_file = "cmgbus"
                    spawn_agents = self.get_bus_agents(agents)
                    psrio_object_filename = self.add_psrio_command(
                    pathname, command, levels, "-" + spw, spawn_file, spawn_agents
                )
                    self.remove_parquet(psrio_object_filename)

            psrio_object_filename = self.add_psrio_command(
                pathname, command, levels, "", file, agents
            )
            self.remove_parquet(psrio_object_filename)

    def add_psrio_command(self, pathname, command, levels, spawn, file, agents) -> str:
        psrio_command = PSRIOCommand(
            self.study, pathname, command, levels, spawn, file, agents
        )
        psrio_object_filename = (
            DICT_PSRFILE_PSRIOOBJECT[psrio_command.file].object_filename
            + levels
            + spawn
        )
        self.psrio_commands[psrio_object_filename].append(psrio_command)
        return psrio_object_filename

    def remove_parquet(self, psrio_object_filename: str):
        parquet_filename = os.path.join(
            self.pathname, psrio_object_filename + ".parquet"
        )
        if os.path.exists(parquet_filename):
            os.remove(parquet_filename)


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
            print(f"Failed to get RefBus: {e}")
            return None  # type: ignore

    def get_bus_agents(self, agents_string) -> str:
        agents_list = agents_string.split(";")
        bus_agents_list = [self.gen_bus_dict[agent] for agent in agents_list]
        return ";".join(bus_agents_list)

    def run_psrio_command(self):
        for psrio_object_filename, psrio_command_list in self.psrio_commands.items():
            df = pd.DataFrame()
            for psrio_command in psrio_command_list:
                if psrio_command.command == "Parquet":
                    df = pd.concat([df, psrio_command.bin_to_parquet()], axis=1)

            parquet_pathname = os.path.join(
                self.pathname, psrio_object_filename + ".parquet"
            )
            df.to_parquet(parquet_pathname)


class PSRIOCasesList:
    def __init__(self):

        psrio_commands: defaultdict[str, list[str]] = defaultdict(list)
        with open(os.path.join("psrio_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline().strip():
                line = [item.strip() for item in line.split(",")]
                command, pathname, levels, spawn, file, agents = line
                psrio_commands_strings = ",".join(
                    [command, levels, spawn, file, agents]
                )
                psrio_commands[pathname].append(psrio_commands_strings)

        self.psrio_cases_list: List[PSRIOCase] = []
        for pathname, psrio_commands_strings in psrio_commands.items():
            self.psrio_cases_list.append(PSRIOCase(pathname, psrio_commands_strings))

    def get_cases(self) -> List[PSRIOCase]:
        return self.psrio_cases_list
