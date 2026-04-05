from collections import defaultdict
from typing import List
import pandas as pd
import os
import psr.factory
from PSRTools.Parameters import DICT_PSRFILE_PSRIOOBJECT
from PSRTools.Parameters import DICT_PSRPLANTCSV_PSRIOOBJECT
from PSRTools.Parameters import LIST_PSRIOOBJECT
from PSRTools.PSRIOCommand import PSRIOCommand

class PSRIOCasesList():
    def __init__(self):

        psrio_cases_dict = defaultdict(list)
        with open(os.path.join("psrio_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline().strip():
                line = [item.strip() for item in line.split(",")]
                command, pathname, levels, file, agents = line
                psrio_commands_strings = ",".join([command, levels, file, agents])
                psrio_cases_dict[pathname].append(psrio_commands_strings)

        self.psrio_cases_list = []
        for pathname, psrio_commands_strings in psrio_cases_dict.items():
            self.psrio_cases_list.append(PSRIOCase(pathname, psrio_commands_strings))

    def get_cases(self) -> List:
        return self.psrio_cases_list


class PSRIOCase:

    def __init__(self, pathname: str, psrio_commands_strings: List[str]):
        self.pathname = pathname
        self.study = psr.factory.load_study(pathname)
        self.psrio_cases_dict = defaultdict(list)

        for string in psrio_commands_strings:
            command, levels, file, agents = string.split(",")
            psrio_command = PSRIOCommand(self.study, pathname, command, levels, file, agents)
            psrio_object_filename = DICT_PSRFILE_PSRIOOBJECT[psrio_command.file].object_filename + levels
            self.psrio_cases_dict[psrio_object_filename].append(psrio_command)

        for psrio_object_filename in self.psrio_cases_dict:
            parquet_filename = os.path.join(self.pathname, psrio_object_filename + ".parquet")
            if os.path.exists(parquet_filename): 
                os.remove(parquet_filename)

        # for csv_file in DICT_PSRPLANTCSV_PSRIOOBJECT:
        #     csv_filepath = os.path.join(self.pathname, csv_file + ".csv")
        #     print(f"{csv_filepath=}")
        #     with open(csv_filepath, "r", encoding="utf-8") as f:
        #         f.readline()
        #         f.readline()
        #         for line in f:
        #             plant, *_ = line.strip().split(",")
        #             plant = plant.strip()
        #             psrio_bus = self.getBus(plant)
        #             print(plant)

        gen_bus_filepath = os.path.join(self.pathname, 'gen_bus.csv')
        with open(gen_bus_filepath, "w", encoding="utf-8") as f:
            f.write("genName,genCode,busName,busCode\n")
            for psrio_object in LIST_PSRIOOBJECT:
                plants = self.study.get(psrio_object)
                assert isinstance(plants, list)
                for plant in plants:
                    bus = self.get_bus(plant)
                    f.write(f"{plant.name}, {plant.code}, {bus.name}, {bus.code}\n")

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



    def run(self):
        for psrio_object_filename, psrio_command_list in self.psrio_cases_dict.items():
            df = pd.DataFrame()
            for psrio_command in psrio_command_list:
                if psrio_command.command == "Parquet":
                    df = pd.concat([df, psrio_command.bin_to_parquet()], axis=1)

            parquet_pathname = os.path.join(
                self.pathname, psrio_object_filename + ".parquet"
            )
            df.to_parquet(parquet_pathname)