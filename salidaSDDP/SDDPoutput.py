import psr.factory
import psr.cloud
import pandas as pd
import os
import time

client = psr.cloud.Client()

case_name = "2026-01Erhb3"
case_path = os.path.join(
    "C:/", 
    "Users", 
    "mamaro", 
    "OneDrive - Colbun S.A", 
    "Gerencia de Planificación - General", 
    "Chile", 
    "SDDP", 
    "2026", 
    case_name
    )

case = psr.cloud.Case(data_path=case_path,
                      program="SDDP",
                      program_version="17.3.12",
                      name=case_name,
                      parent_case_id="213417",
                      price_optimized=True,
                      execution_type="Default",
                      number_of_processes=64,
                      memory_per_process_ratio="2:1"
                      )

try:
    case_id = client.run_case(case)
    status, status_msg = client.get_status(case_id)
    while True:
        if status in psr.cloud.status.FAULTY_TERMINATION_STATUS:
            print(f"Case {case_id} failed with status: {status}.")
            break

        if status in psr.cloud.status.FINISHED_STATUS:
            print(f"Case {case_id} finished.")
            break

        time.sleep(60)
        previous_status = status
        status, status_msg = client.get_status(case_id)

        if status != previous_status:
            print(f"Case {case_id} status changed from {previous_status} to {status}.")
            previous_status = status
except psr.cloud.CloudInputError as e:
    print(f"Error running case: {e}")

if str(status) == "ExecutionStatus.SUCCESS":
    client.download_results(
        case_id, 
        case_path, 
        ["cmgbus.hdr", "gerhid.hdr", "cmgbus.bin", "gerhid.bin"], 
        []
    )

study = psr.factory.load_study(
    case_path
    )

load_options = psr.factory.create("DataFrameLoadOptions")

hydro_plants = study.get("HydroPlant")
dict_hydro_plants = {}

for plant in hydro_plants:
    generators = plant.get("RefGenerators")
    plant_buses = []
    for generator in generators:
        plant_buses.append(generator.get("RefBus"))
    dict_hydro_plants[plant.name.strip()] = [plant.code, plant_buses[0].name.strip(), plant_buses[0].code]

# gen_agents = ["Colbun", "SanClemente", "Chiburgo", "Machicura", "SanIgnacio"]
gen_agents = ["Colbun", "SanClemente", "Chiburgo", "Machicura", "SanIgnacio", "Colbun_bb", "Colbun_bg"]
# gen_agents = ["Colbun", "SanClemente", "Chiburgo", "Machicura", "SanIgnacio", "MauleB", "MauleG"]
load_options.set("FilterAgents", gen_agents)

df_f_gen_agents = psr.factory.load_dataframe(os.path.join(case_path, "gerhid.hdr"), options=load_options)
df_p_gen_agents = df_f_gen_agents.to_pandas()
# df_p_gen_agents = df_p_gen_agents.groupby(["year", "month", "hour"]).mean()
df_p_gen_agents.columns = [dict_hydro_plants[name][0] for name in df_p_gen_agents.columns.to_list()]
df_p_gen_agents.to_parquet(os.path.join(case_path, "gerhid.parquet"))
    
with open(os.path.join(case_path, "hydro_plants.csv"), "w") as f:
    print("genName,genCode,busName,busCode", file=f)
    for gen in gen_agents:
        print(gen, *dict_hydro_plants[gen], sep=',', file=f)

buses = study.get("Bus")
dict_buses = {bus.name.strip(): bus.code for bus in buses}

bus_agents = [dict_hydro_plants[gen_agent][1] for gen_agent in gen_agents]
bus_agents = list(set(bus_agents))
load_options.set("FilterAgents", bus_agents)

df_f_bus_agents = psr.factory.load_dataframe(os.path.join(case_path, "cmgbus.hdr"), options=load_options)
df_p_bus_agents = df_f_bus_agents.to_pandas()
# df_p_bus_agents = df_p_bus_agents.groupby(["year", "month", "hour"]).mean()
df_p_bus_agents.columns = [dict_buses[name] for name in df_p_bus_agents.columns.to_list()]
df_p_bus_agents.to_parquet(os.path.join(case_path, "cmgbus.parquet"))

with open(os.path.join(case_path, "buses.csv"), "w") as f:
    print("busName,busCode", file=f)
    for bus in bus_agents:
        print(bus, dict_buses[bus], sep=',', file=f)