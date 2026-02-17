import psr.factory

study = psr.factory.load_study(
    r"C:\Users\mamaro\OneDrive - Colbun S.A\Gerencia de Planificación - General\Chile\SDDP\2025\2025-10Xrh"
    )
thermal_plants = study.get("ThermalPlant")
batteries = study.get("Battery")
hydro_plants = study.get("HydroPlant")
renewable_plants = study.get("RenewablePlant")
dict_plants = {}
for plant_list in [hydro_plants, thermal_plants, renewable_plants, batteries]:
    if isinstance(plant_list, list):
        for plant in plant_list:
            dict_plants[plant.name.strip()] = plant.code
print(f"dict_plants = {dict_plants}\n") 

buses = study.get("Bus")
dict_buses = {}
if isinstance(buses, list):
    for bus in buses:
        dict_buses[bus.name.strip()] = bus.code
print(f"dict_buses = {dict_buses}\n") 
