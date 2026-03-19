from collections import namedtuple
from typing import NamedTuple

class SDDPCase(NamedTuple):
    casename: str
    pathname: str
    parent: str | None
    input_files: str
    output_files: str
    id: int
    
    def __str__(self):
        result = [str(item) for item in self]
        return ",".join(result)
