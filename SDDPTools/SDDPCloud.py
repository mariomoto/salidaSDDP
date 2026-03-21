from pathlib import Path
from collections import namedtuple
from typing import List, NamedTuple
import psr.cloud
import psr.cloud.status
import time
import numpy as np
from SDDPTools.SDDPCommand import SDDPCommand

class SDDPCommandsList(List[SDDPCommand]):
    def __init__(self):
        super().__init__()
        with open("sddp_commands.csv", "r", encoding="utf-8") as f:
            _ = next(f)
            while (line := f.readline()):
                line = [item.strip() for item in line.split(",")]
                command, pathname, parent_id, id, output_files = line
                casename = Path(pathname).name
                id = int(id or '0')
                self.append(SDDPCommand(command, casename, pathname, parent_id, id, output_files))

class SDDPStudyCase():
    def __init__(self, client: psr.cloud.Client, sddp_command: SDDPCommand):
        self.client = client
        self.sddp_command = sddp_command
        self.case: psr.cloud.Case | None = None 

    def run_study(self):
        self.case = psr.cloud.Case(
                            data_path=self.sddp_command.pathname,
                            program="SDDP",
                            program_version="17.3.12",
                            name=self.sddp_command.casename,
                            parent_case_id=self.sddp_command.parent_id,
                            price_optimized=True,
                            execution_type="Default",
                            number_of_processes=64,
                            memory_per_process_ratio="2:1"
                        )
        print(f"{__name__}: Study '{self.sddp_command.casename}' created.")
        status = None
        try:
            assert isinstance(self.case, psr.cloud.Case)
            self.sddp_command = self.sddp_command._replace(id = self.client.run_case(self.case))
            status, status_msg = self.client.get_status(self.sddp_command.id)

            while status not in psr.cloud.status.FINISHED_STATUS: # type: ignore
                time.sleep(60)
                previous_status = status
                status, status_msg = self.client.get_status(self.sddp_command.id)

                if status != previous_status:
                    print(f"Case {self.sddp_command.id} status changed from {previous_status} to {status}.")
                    previous_status = status
        except psr.cloud.CloudInputError as e:
            print(f"Error running case: {e}")
        finally:
            return status

    def download_files(self):
        if (self.sddp_command.id):
            status, status_msg = self.client.get_status(self.sddp_command.id)
            if str(status) == "ExecutionStatus.SUCCESS":
                output_files = self.sddp_command.output_files
                output_files = [f"{name}.{ext}" for name in output_files.split(';') for ext in ['hdr', 'bin']]
                self.client.download_results(
                    self.sddp_command.id, 
                    self.sddp_command.pathname, 
                    output_files, 
                    []
                )

