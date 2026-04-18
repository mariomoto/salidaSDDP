from typing import NamedTuple

    
class PsrioObjectInfo(NamedTuple):
    object_type: str
    object_filename: str
    operation: str
    factor: float
    
DICT_PSRFILE_PSRIOOBJECT = {
    "cmgbus": PsrioObjectInfo("Bus", "cmgbus", "mean", 1),
    "gerbat": PsrioObjectInfo("Battery", "EnergyOutputs", "sum", 1000),
    "gergnd": PsrioObjectInfo("RenewablePlant", "EnergyOutputs", "sum", 1),
    "gerhid": PsrioObjectInfo("HydroPlant", "EnergyOutputs", "sum", 1),
    "gerter": PsrioObjectInfo("ThermalPlant", "EnergyOutputs", "sum", 1),
    "gbcmgb": PsrioObjectInfo("Battery", "Sales", "sum", 1000),
    "ggcmgb": PsrioObjectInfo("RenewablePlant", "Sales", "sum", 1),
    "ghcmgb": PsrioObjectInfo("HydroPlant", "Sales", "sum", 1),
    "gtcmgb": PsrioObjectInfo("ThermalPlant", "Sales", "sum", 1),
    "ingtci": PsrioObjectInfo("Circuit", "ingtci", "sum", 1),
    "demxba": PsrioObjectInfo("Bus", "demxba", "sum", 1),
    "demand": PsrioObjectInfo("System", "demand", "sum", 1),
    "coster": PsrioObjectInfo("ThermalPlant", "coster", "sum", 1),
    "cosarr": PsrioObjectInfo("ThermalPlant", "cosarr", "sum", 1),
    "trstup": PsrioObjectInfo("ThermalPlant", "trstup", "mean", 1),
}

LIST_PSRIOOBJECT = [
    "Battery",
    "HydroPlant",
    "RenewablePlant",
    "ThermalPlant",
]

PSRIO_COMMANDS = ["Parquet"]
