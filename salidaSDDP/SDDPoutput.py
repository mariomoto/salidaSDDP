import psr.factory

study = psr.factory.load_study(
    r"C:\Users\mamaro\OneDrive - Colbun S.A\Gerencia de Planificación - General\Chile\SDDP\2025\2025-10Xrh"
    )
# print(psr.factory.help())
thermal_plants = study.get("ThermalPlant")
# print(type(thermal_plants))
if isinstance(thermal_plants, list):
    print(len(thermal_plants))
    # print(thermal_plants[0])
    # print(dir(thermal_plants[0]))
    print(thermal_plants[0].type)
    # print(thermal_plants[0].help())
    print(thermal_plants[0].get('InstalledCapacity'))
# batteries = study.get("Battery")
# hydro_plants = study.get("HydroPlant")
# renewable_plants = study.get("RenewablePlant")
# buses = study.get("Bus")
# print(thermal_plants)
# print(batteries[0])
# print(hydro_plants[0])
# print(renewable_plants[0])
# print(buses[0])
# print(len(thermal_plants))
# print(len(batteries))
# print(len(hydro_plants))
# print(len(renewable_plants))
# print(len(buses))

