import psr.factory

study = psr.factory.load_study(
    r"C:\Users\mamaro\OneDrive - Colbun S.A\Gerencia de Planificación - General\Chile\SDDP\2025\2025-10Xrh"
    )
thermal_plants = study.get("ThermalPlant")
if isinstance(thermal_plants, list):
    dict_thermal_plants = {plant.name.strip(): plant.code for plant in thermal_plants}  
    print(dict_thermal_plants) 

