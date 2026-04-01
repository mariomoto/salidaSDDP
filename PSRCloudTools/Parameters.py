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
    levels: str
    file: str
    agents: str
    
    def __str__(self):
        result = [str(item) for item in self]
        return ",".join(result)
    
class PsrioObjectInfo(NamedTuple):
    object_type: str
    object_filename: str
    
DICT_FILE_PSRIOOBJECT = {
    "cmgbus": PsrioObjectInfo("Bus", "cmgbus"),
    "gerbat": PsrioObjectInfo("Battery", "EnergyOutputs"),
    "gergnd": PsrioObjectInfo("RenewablePlant", "EnergyOutputs"),
    "gerhid": PsrioObjectInfo("HydroPlant", "EnergyOutputs"),
    "gerter": PsrioObjectInfo("ThermalPlant", "EnergyOutputs"),
    "gbcmgb": PsrioObjectInfo("Battery", "Sales"),
    "ggcmgb": PsrioObjectInfo("RenewablePlant", "Sales"),
    "ghcmgb": PsrioObjectInfo("HydroPlant", "Sales"),
    "gtcmgb": PsrioObjectInfo("ThermalPlant", "Sales"),
    "ingtci": PsrioObjectInfo("Circuit", "ingtci"),
    "demxba": PsrioObjectInfo("Bus", "demxba"),
    }
