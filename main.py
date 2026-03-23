import SDDPTools.SDDPCloud as sc
import SDDPTools.SDDPParquet as sp
from SDDPTools.Parameters import SDDPCommand
import psr.cloud


def run(sddp_command: SDDPCommand):
    client = psr.cloud.Client()

    study_case = sc.SDDPStudyCase(client, sddp_command)
    study_case.run_study()


def download(sddp_command: SDDPCommand):
    client = psr.cloud.Client()

    study_case = sc.SDDPStudyCase(client, sddp_command)
    study_case.download_files()


def parquet(sddp_command: SDDPCommand):

    study_case = sp.SDDPParquet(sddp_command)
    study_case.ger_bin_to_parquet()


sddp_commands_list = sc.SDDPCommandsList()

for sddp_command in sddp_commands_list:
    match sddp_command.command:
        case "Run":
            run(sddp_command)
        case "Download":
            download(sddp_command)
        case "Parquet":
            parquet(sddp_command)
