from pathlib import Path
from collections import namedtuple
from typing import List, NamedTuple
import psr.cloud
import psr.cloud.status
import time
import numpy as np
from SDDPTools.SDDPCase import SDDPCase

class SDDPCasesInputList(List[SDDPCase]):
    def __init__(self):
        super().__init__()
        with open("s2pIn.csv", "r", encoding="utf-8") as f:
            _ = next(f)
            while (line := f.readline()):
                line = [item.strip() for item in line.split(",")]
                pathname, parent, input_files, output_files = line
                if parent == "":
                    parent = None
                casename = Path(pathname).name
                id = 0
                self.append(SDDPCase(casename, pathname, parent, input_files, output_files, id))


class SDDPCasesOutputList(List[SDDPCase]):
    def __init__(self):
        super().__init__()
        with open("s2pOut.csv", "r", encoding="utf-8") as f:
            _ = next(f)
            while (line := f.readline()):
                line = [item.strip() for item in line.split(",")]
                casename, pathname, parent, input_files, output_files, id = line
                if parent == "":
                    parent = None
                if id.isnumeric():
                    id = int(id)
                else:
                    id = 0
                self.append(SDDPCase(casename, pathname, parent, input_files, output_files, id))

class SDDPStudyCase():
    def __init__(self, client: psr.cloud.Client, sddp_case: SDDPCase):
        self.client = client
        self.sddp_case = sddp_case
        self.case: psr.cloud.Case | None = None 

    def upload_study(self) -> psr.cloud.Case | None:
        self.sddp_case = self.sddp_case._replace(
                        id = psr.cloud.Case(
                            data_path=self.sddp_case.pathname,
                            program="SDDP",
                            program_version="17.3.12",
                            name=self.sddp_case.casename,
                            parent_case_id=self.sddp_case.parent,
                            price_optimized=True,
                            execution_type="Default",
                            number_of_processes=64,
                            memory_per_process_ratio="2:1"
                        ))    
        print(f"{__name__}: Study '{self.sddp_case.casename}' created.")

    def run_study(self):
        status = None
        try:
            assert isinstance(self.case, psr.cloud.Case)
            self.sddp_case = self.sddp_case._replace(id = self.client.run_case(self.case))
            status, status_msg = self.client.get_status(self.sddp_case.id)

            while status not in psr.cloud.status.FINISHED_STATUS: # type: ignore
                time.sleep(60)
                previous_status = status
                status, status_msg = self.client.get_status(self.sddp_case.id)

                if status != previous_status:
                    print(f"Case {self.sddp_case.id} status changed from {previous_status} to {status}.")
                    previous_status = status
        except psr.cloud.CloudInputError as e:
            print(f"Error running case: {e}")
        finally:
            return status

    def download_files(self):
        if (self.sddp_case.id):
            status, status_msg = self.client.get_status(self.sddp_case.id)
            if str(status) == "ExecutionStatus.SUCCESS":
                output_files = self.sddp_case.output_files
                output_files = [f"{name}.{ext}" for name in output_files.split(';') for ext in ['hdr', 'bin']]
                self.client.download_results(
                    self.sddp_case.id, 
                    self.sddp_case.pathname, 
                    output_files, 
                    []
                )

