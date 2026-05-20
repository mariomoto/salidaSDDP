from pathlib import Path
from typing import List
import time
import os
import psr.cloud
import psr.cloud.status
from utils import my_print, convert_to_short_path


class PSRCloudCommand:

    def __init__(
        self,
        command: str,
        version: str,
        optimized: str,
        pathname: str,
        parent_id: str | None,
        id: int,
        output_files: str,
    ):
        self.command = command
        self.casename = Path(pathname).name
        self.pathname = pathname
        self.parent_id = parent_id
        self.id = id
        self.optimized = True if optimized.upper() == "TRUE" else False
        self.output_files = output_files
        self.version = version


class PSRCloudCommandsList(List[PSRCloudCommand]):
    def __init__(self, output_folder: str):
        super().__init__()
        with open(
            os.path.join(output_folder, "psrcloud_commands.csv"), "r", encoding="latin-1"
        ) as f:
            _ = next(f)
            while line := f.readline():
                line = [item.strip() for item in line.split(",")]
                command, version, optimized, pathname, parent_id, id, output_files = line
                parent_id = parent_id or None
                id = int(id or "0")
                if not os.path.isabs(pathname):
                    raise ValueError(f"pathname must be an absolute path, got: {pathname!r}")
                pathname = convert_to_short_path(pathname)
                self.append(
                    PSRCloudCommand(
                        command, version, optimized, pathname, parent_id, id, output_files
                    )
                )


class PSRCloudCase:
    def __init__(self, client: psr.cloud.Client, psrcloud_command: PSRCloudCommand):
        self.client = client
        self.psrcloud_command = psrcloud_command
        self.case: psr.cloud.Case | None = None

    def run_study(self):
        try:
            status = self.try_run_study()
        except psr.cloud.CloudError as e:
            my_print(f"{self.psrcloud_command.casename}: {e}")
            self.psrcloud_command.optimized = False
            my_print(f"{self.psrcloud_command.casename}: Se ejecuta sin optimización de precio.")
            status = self.try_run_study()

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
                    self.psrcloud_command.id,
                    self.psrcloud_command.pathname,
                    output_files,
                    [],
                )

    def try_run_study(self):
        self.case = psr.cloud.Case(
            name=self.psrcloud_command.casename,
            data_path=self.psrcloud_command.pathname,
            program="SDDP",
            program_version=self.psrcloud_command.version,
            execution_type="Operation Planning (Default)",
            memory_per_process_ratio="2:1",
            price_optimized=self.psrcloud_command.optimized,
            number_of_processes=64,
            repository_duration=2,
            budget="",
            parent_case_id=self.psrcloud_command.parent_id,
        )
        my_print(f"Study '{self.psrcloud_command.casename}' created.")
        status = None
        try:
            assert isinstance(self.case, psr.cloud.Case)
            case_id = self.client.run_case(self.case)
            self.psrcloud_command.id = case_id
            status, status_msg = self.client.get_status(self.psrcloud_command.id)

            poll_interval = 1800
            start = (
                time.monotonic() - poll_interval
            )  # fire immediately on first iteration
            previous_status = status
            while status not in psr.cloud.status.FINISHED_STATUS:  # type: ignore
                time.sleep(60)
                status, status_msg = self.client.get_status(
                    self.psrcloud_command.id, quiet=True
                )
                if time.monotonic() - start >= poll_interval:
                    my_print(f"{self.psrcloud_command.casename}({self.psrcloud_command.id}): {status_msg}")
                    start = (
                        time.monotonic()
                    )  # reset AFTER the poll, not inside the branch that detects elapsed time

                if status != previous_status:
                    my_print(
                        f"{self.psrcloud_command.casename}({self.psrcloud_command.id}) status changed from {previous_status} to {status}."
                        )
                    previous_status = status
        except psr.cloud.CloudInputError as e:
            my_print(f"{self.psrcloud_command.casename}({self.psrcloud_command.id}): {e}")
