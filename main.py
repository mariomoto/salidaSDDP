import concurrent.futures
import SDDPTools.SDDPCloud as sc
import SDDPTools.SDDPParquet as sp
from SDDPTools.Parameters import SDDPCloudCommand
import psr.cloud


def run(sddp_cloud_command: SDDPCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.SDDPStudyCase(client, sddp_cloud_command)
    study_case.run_study()


def download(sddp_cloud_command: SDDPCloudCommand):
    client = psr.cloud.Client()

    study_case = sc.SDDPStudyCase(client, sddp_cloud_command)
    study_case.download_files()


def parquet(sddp_cloud_command: SDDPCloudCommand):

    study_case = sp.SDDPParquet(sddp_cloud_command)
    study_case.ger_bin_to_parquet()     


sddp_cloud_commands_list = sc.SDDPCloudCommandsList()

if __name__ == '__main__':
    # ThreadPoolExecutor doesn't have the Windows spawning issue
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for sddp_cloud_command in sddp_cloud_commands_list:
            match sddp_cloud_command.command:
                case "Run":
                    future = executor.submit(run, sddp_cloud_command)
                    futures.append(future)
                case "Download":
                    download(sddp_cloud_command)
                case "Parquet":
                    parquet(sddp_cloud_command)
