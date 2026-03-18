from pathlib import Path
from collections import namedtuple
from typing import List
import psr.cloud
import time

SDDPCase = namedtuple("SDDPCase", ["casename", "pathname", "parent", "input_files", "output_files"])

class SDDPCasesList(List[SDDPCase]):
    def __init__(self):
        super().__init__()
        with open("s2pIn.csv", "r", encoding="utf-8") as f:
            _ = next(f)
            while (line := f.readline()):
                line = [item.strip() for item in line.split(",")]
                line[0] = Path(line[0]) # type: ignore
                casename, pathname, parent, input_files, output_files = line[0].name, *line
                if parent == "":
                    parent = None
                self.append(SDDPCase(casename, pathname, parent, input_files, output_files))

class SDDPStudyCase(psr.cloud.Case):
    def __init__(self, client: psr.cloud.Client, sddp_case: SDDPCase):
        self.client = client
        self.sddp_case = sddp_case
        self.case_id = None
        super().__init__(data_path=str(sddp_case.pathname),
                      program="SDDP",
                      program_version="17.3.12",
                      name=sddp_case.casename,
                      parent_case_id=sddp_case.parent,
                      price_optimized=True,
                      execution_type="Default",
                      number_of_processes=64,
                      memory_per_process_ratio="2:1"
                      )
        print(f"{__name__}: Study {self.sddp_case.casename} created.")

    def run_study(self):
        try:
            self.case_id = self.client.run_case(self)
            status, status_msg = self.client.get_status(self.case_id)

            while status not in psr.cloud.status.FINISHED_STATUS: # type: ignore
                time.sleep(60)
                previous_status = status
                status, status_msg = self.client.get_status(self.case_id)

                if status != previous_status:
                    print(f"Case {self.case_id} status changed from {previous_status} to {status}.")
                    previous_status = status
        except psr.cloud.CloudInputError as e:
            print(f"Error running case: {e}")
        else:
            if str(status) == "ExecutionStatus.SUCCESS":
                output_files = self.sddp_case.output_files
                output_files = [f"{name}.{ext}" for name in output_files.split(';') for ext in ['hdr', 'bin']]
                self.client.download_results(
                    self.case_id, 
                    self.sddp_case.pathname, 
                    output_files, 
                    []
                )
