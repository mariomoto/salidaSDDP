from typing import NamedTuple

class PSRCloudCommand(NamedTuple):
    command: str
    casename: str
    pathname: str
    parent_id: str | None
    id: int
    output_files: str
    
    def __str__(self):
        result = [str(item) for item in self]
        return ",".join(result)
    
class PSRIOCommand(NamedTuple):
    command: str
    pathname: str
    file: str
    agents: str
    
    def __str__(self):
        result = [str(item) for item in self]
        return ",".join(result)
    
DICT_FILE_PSRIOOBJECT = {
    "cmgbus": "Bus"
    "gerbat": "Battery",
    "gergnd": "RenewablePlant",
    "gerhid": "HydroPlant",
    "gerter": "ThermalPlant",
    "gbcmgb": "Battery",
    "ggcmgb": "RenewablePlant",
    "ghcmgb": "HydroPlant",
    "gtcmgb": "ThermalPlant",
    }
