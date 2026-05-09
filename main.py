import threading
import PSRTools.PSRCloudCase as sc
import PSRTools.PSRIOCase as sio
import psr.cloud
import psr.factory
import sys
from  utils import choose_directory_with_history


def run(client: psr.cloud.Client, psrcloud_command: sc.PSRCloudCommand):
    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.run_study()


def download(client: psr.cloud.Client, psrcloud_command: sc.PSRCloudCommand):
    study_case = sc.PSRCloudCase(client, psrcloud_command)
    study_case.download_files()


def run_then_download(client: psr.cloud.Client, psrcloud_command: sc.PSRCloudCommand):
    run(client, psrcloud_command)
    download(client, psrcloud_command)


if __name__ == "__main__":

    client = psr.cloud.Client(quiet = True)

    psr.factory.set_setting("PASSKEY", "1:MAmaro@colbun.cl:2026-07-07.w45LxCG5KtB_ArtYb8e9EKfxcroefiPCAYC6TlJ-OGF0sYn6LbUn78KdirsIKqA19iczOSLhW5BMvJ9Et-MfBw")

    directory = choose_directory_with_history()
    if not directory:
        sys.exit()
    psrcloud_commands_list = sc.PSRCloudCommandsList(directory)
    threads = []
    for psrcloud_command in psrcloud_commands_list:
        match psrcloud_command.command:
            case "Run":
                thread = threading.Thread(target=run, args=(client, psrcloud_command,))
                thread.start()
                threads.append(thread)
            case "RunDownload":
                thread = threading.Thread(target=run_then_download, args=(client, psrcloud_command,))
                thread.start()
                threads.append(thread)
            case "Download":
                download(client, psrcloud_command)

    for thread in threads:
        thread.join()

    psrio_cases_list = sio.PSRIOCasesList(directory)

    for psrio_case in psrio_cases_list.get_cases():
        psrio_case.run_psrio_commands()
