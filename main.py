import SDDP2Parquet.s2p as s2p
import psr.cloud

client = psr.cloud.Client()

sddp_cases_list = s2p.SDDPCasesList()

f = open('s2pOut.csv', "w", encoding='utf-8')
f.write("name,path,parent,input_files,output_files,id\n")

for sddp_case in sddp_cases_list:
    print(sddp_case)
    study_case = s2p.SDDPStudyCase(client, sddp_case)
    study_case.run_study()
    f.write(",".join(list(map(str, sddp_case))) + "," + str(study_case.case_id) + "\n")
    study_case.download_files()

f.close()