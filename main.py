import os
import logging
import threading
import PSRTools.PSRCloudCase as sc
import PSRTools.PSRIOCase as sio
import psr.cloud
import psr.factory
import sys
from utils import choose_directory_with_history, convert_to_short_path
import ctypes

# Suppress the massive '--- Logging error ---' tracebacks from psr.cloud's
# console output containing Latin-1 characters that cp1252 can't encode.
_original_handle_error = logging.Handler.handleError

def _quiet_handle_error(self, record):  # type: ignore[no-untyped-def]
    _, exc, _ = sys.exc_info()
    if not isinstance(exc, UnicodeEncodeError):
        _original_handle_error(self, record)

logging.Handler.handleError = _quiet_handle_error  # type: ignore[assignment]


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

    output_folder = choose_directory_with_history()

    if not output_folder:
        sys.exit()

    output_folder = convert_to_short_path(output_folder)

    with open(os.path.join("c:\\", "PSR", "passkey.txt"), "r") as f:
        passkey = f.read().strip()

    psr.factory.set_setting("PASSKEY", passkey)

    try:
        psrcloud_commands_list = sc.PSRCloudCommandsList(output_folder)
    except ValueError as e:
        ctypes.windll.user32.MessageBoxW(0, str(e), "Error", 0x10)
        sys.exit(1)

    client = (
        psr.cloud.Client()
        if any(
            cmd.command in {"Run", "RunDownload", "Download"}
            for cmd in psrcloud_commands_list
        )
        else None
    )
    threads = []
    for psrcloud_command in psrcloud_commands_list:
        match psrcloud_command.command:
            case "Run":
                thread = threading.Thread(
                    target=run,
                    args=(
                        client,
                        psrcloud_command,
                    ),
                )
                thread.start()
                threads.append(thread)
            case "RunDownload":
                thread = threading.Thread(
                    target=run_then_download,
                    args=(
                        client,
                        psrcloud_command,
                    ),
                )
                thread.start()
                threads.append(thread)
            case "Download":
                download(
                    client, psrcloud_command  # pyright: ignore[reportArgumentType]
                )

    for thread in threads:
        thread.join()
    try:
        psrio_cases_list = sio.PSRIOCasesList(output_folder)
    except ValueError as e:
        ctypes.windll.user32.MessageBoxW(0, str(e), "Error", 0x10)
        sys.exit(1)

    for psrio_case in psrio_cases_list.get_cases():
        psrio_case.run_psrio_commands()
