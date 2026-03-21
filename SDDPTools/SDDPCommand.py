from collections import namedtuple
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
