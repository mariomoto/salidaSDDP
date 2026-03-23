from typing import NamedTuple

class SDDPCommand(NamedTuple):
    command: str
    casename: str
    pathname: str
    parent_id: str | None
    id: int
    output_files: str
    
    def __str__(self):
        result = [str(item) for item in self]
        return ",".join(result)
    
class PLANT(NamedTuple):
    plant_code: int
    bus_name: str
    bus_code: int

DICT_TECH_PLANT = {
    "gerbat": "Battery",
    "gergnd": "RenewablePlant",
    "gerhid": "HydroPlant",
    "gerter": "ThermalPlant"
    }
