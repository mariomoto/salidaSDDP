from typing import NamedTuple

    
class PsrioObjectInfo(NamedTuple):
    object_type: str
    object_filename: str
    
DICT_PSRFILE_PSRIOOBJECT = {
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

DICT_PSRPLANTCSV_PSRIOOBJECT = {
    "atbatt": "Battery",
    "athidr": "HydroPlant",
    "atrenw": "RenewablePlant",
    "atterm": "ThermalPlant",
}

LIST_PSRIOOBJECT = [
    "Battery",
    "HydroPlant",
    "RenewablePlant",
    "ThermalPlant",
]

PSRIO_COMMANDS = ["Parquet"]
