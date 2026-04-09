import tkinter as tk
from tkinter.filedialog import askdirectory
import concurrent.futures
import PSRTools.PSRCloudCase as sc
import PSRTools.PSRIOCase as sio
import psr.cloud


def run(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.run_study()


def download(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.download_files()


if __name__ == '__main__':
    # tk.Tk().withdraw() # part of the import if you are not using other tkinter functions

    # directory = askdirectory()

    psrcloud_commands_list = sc.PSRCloudCommandsList()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
    for psrcloud_command in psrcloud_commands_list:
        match psrcloud_command.command:
            case "Run":
                future = executor.submit(run, psrcloud_command)
                futures.append(future)
            case "RunDownload":
                run(psrcloud_command)
                download(psrcloud_command)
            case "Download":
                download(psrcloud_command)

    psrio_cases_list = sio.PSRIOCasesList()

    for psrio_case in psrio_cases_list.get_cases():
            psrio_case.run_psrio_commands()