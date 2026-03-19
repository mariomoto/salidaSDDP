import SDDPTools.SDDPCloud as sc
import SDDPTools.BinToParquet as b2c
import psr.cloud

def upload_run_and_download():
    client = psr.cloud.Client()

    sddp_cases_output_list = sc.SDDPCasesInputList()

    f = open('scOut.csv', "w", encoding='utf-8')
    f.write("name,path,parent,input_files,output_files,id\n")

    for sddp_case in sddp_cases_output_list:
        print(sddp_case)
        study_case = sc.SDDPStudyCase(client, sddp_case)
        study_case.upload_study()
        study_case.run_study()
        f.write(str(sddp_case) + "," + str(study_case.sddp_case.id) + "\n")
        study_case.download_files()

    f.close()

def download():
    client = psr.cloud.Client()

    sddp_cases_output_list = sc.SDDPCasesOutputList()

    for sddp_case in sddp_cases_output_list:
        study_case = sc.SDDPStudyCase(client, sddp_case)
        study_case.download_files()

def to_parquet():
    sddp_cases_output_list = sc.SDDPCasesOutputList()

    for sddp_case in sddp_cases_output_list:
        study_case = b2c.Bin2Parquet(sddp_case)
        study_case.hid_bin_to_parquet()

to_parquet()
