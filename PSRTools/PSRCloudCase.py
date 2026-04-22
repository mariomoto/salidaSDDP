from pathlib import Path
from typing import List
import psr.cloud
import psr.cloud.status
import time
import os

class PSRCloudCommand:

    def __init__(self, 
                 command: str, pathname: str, parent_id: str | None, 
                 id: int, output_files: str
        ):
        self.command = command
        self.casename = Path(pathname).name
        self.pathname = pathname
        self.parent_id = parent_id
        self.id = id
        self.output_files = output_files


class PSRCloudCommandsList(List[PSRCloudCommand]):
    def __init__(self):
        super().__init__()
        with open(os.path.join("psrcloud_commands.csv"), "r", encoding="utf-8") as f:
            _ = next(f)
            while line := f.readline():
                line = [item.strip() for item in line.split(",")]
                command, pathname, parent_id, id, output_files = line
                parent_id = parent_id or None
                id = int(id or "0")
                self.append(
                    PSRCloudCommand(
                        command, pathname, parent_id, id, output_files
                    )
                )


class PSRCloudCase:
    def __init__(self, client: psr.cloud.Client, psrcloud_command: PSRCloudCommand):
        self.client = client
        self.psrcloud_command = psrcloud_command
        self.case: psr.cloud.Case | None = None

    def run_study(self):
        self.case = psr.cloud.Case(
            data_path=self.psrcloud_command.pathname,
            program="SDDP",
            program_version="17.3.12",
            name=self.psrcloud_command.casename,
            parent_case_id=self.psrcloud_command.parent_id,
            price_optimized=True,
            execution_type="Default",
            number_of_processes=64,
            memory_per_process_ratio="2:1",
        )
        print(f"{__name__}: Study '{self.psrcloud_command.casename}' created.")
        status = None
        try:
            assert isinstance(self.case, psr.cloud.Case)
            case_id=self.client.run_case(self.case)
            self.psrcloud_command.id=case_id
            status, status_msg = self.client.get_status(self.psrcloud_command.id)

            num_seconds = 1800
            start = time.monotonic()
            previous_status = status
            while status not in psr.cloud.status.FINISHED_STATUS:  # type: ignore

                if time.monotonic() - start >= num_seconds:
                    start = time.monotonic()
                    status, status_msg = self.client.get_status(self.psrcloud_command.id)

                if status != previous_status:
                    print(
                        f"Case {self.psrcloud_command.id} status changed from {previous_status} to {status}."
                    )
                    previous_status = status

        except psr.cloud.CloudInputError as e:
            print(f"Error running case: {e}")

        return status

    def download_files(self):
        if self.psrcloud_command.id:
            status, status_msg = self.client.get_status(self.psrcloud_command.id)
            if str(status) == "ExecutionStatus.SUCCESS":
                output_files = self.psrcloud_command.output_files
                output_files = [
                    f"{name}.{ext}"
                    for name in output_files.split(";")
                    for ext in ["hdr", "bin"]
                ]
                self.client.download_results(
                    self.psrcloud_command.id, self.psrcloud_command.pathname, output_files, []
                )
