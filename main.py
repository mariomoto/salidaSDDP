import SDDP2Parquet.s2p as s2p
import psr.cloud

client = psr.cloud.Client()
sddp_cases_list = s2p.SDDPCasesList()
for sddp_case in sddp_cases_list:
    print(sddp_case)
    study_case = s2p.SDDPStudyCase(client, sddp_case)
    study_case.run_study()
    pass