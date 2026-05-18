import os
import threading
import PSRTools.PSRCloudCase as sc
import PSRTools.PSRIOCase as sio
import psr.cloud
import psr.factory
import sys
from  utils import choose_directory_with_history
import ctypes


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

    directory = choose_directory_with_history()

    if not directory:
        sys.exit()

    buf = ctypes.create_unicode_buffer(512)
    ctypes.windll.kernel32.GetShortPathNameW(directory, buf, 512)
    directory = buf.value

    client = psr.cloud.Client()
    with open(os.path.join("c:\\", "PSR", "passkey.txt"), "r") as f:
        passkey = f.read().strip()

    psr.factory.set_setting("PASSKEY", passkey)

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
