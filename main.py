import tkinter as tk
from tkinter.filedialog import askdirectory
import concurrent.futures
import PSRCloudTools.PSRCloudCommand as sc
import PSRCloudTools.PSRIOCommand as sio
import psr.cloud


def run(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.run_study()


def download(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.download_files()


def parquet(psrio_command: sio.PSRIOCommand):

    study_case = sio.PSRIOCase(psrio_command)
    study_case.bin_to_parquet()


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

    psrio_commands_list = sio.PSRIOCommandsList()

    for psrio_command in psrio_commands_list:
        match psrio_command.command:
            case "Parquet":
                parquet(psrio_command)
