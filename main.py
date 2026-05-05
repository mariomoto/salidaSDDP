import threading
import PSRTools.PSRCloudCase as sc
import PSRTools.PSRIOCase as sio
import psr.cloud
import sys
from  utils import choose_directory_with_history


def run(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.run_study()


def download(psrcloud_command: sc.PSRCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.download_files()


def run_then_download(psrcloud_command: sc.PSRCloudCommand):
    run(psrcloud_command)
    download(psrcloud_command)


if __name__ == "__main__":
    directory = choose_directory_with_history()
    if not directory:
        sys.exit()
    psrcloud_commands_list = sc.PSRCloudCommandsList(directory)
    threads = []
    for psrcloud_command in psrcloud_commands_list:
        match psrcloud_command.command:
            case "Run":
                thread = threading.Thread(target=run, args=(psrcloud_command,))
                thread.start()
                threads.append(thread)
            case "RunDownload":
                thread = threading.Thread(target=run_then_download, args=(psrcloud_command,))
                thread.start()
                threads.append(thread)
            case "Download":
                download(psrcloud_command)

    for thread in threads:
        thread.join()

    psrio_cases_list = sio.PSRIOCasesList(directory)

    for psrio_case in psrio_cases_list.get_cases():
        psrio_case.run_psrio_commands()
