from __future__ import annotations
from pathlib import Path
from .from_csv import FromCsvMixin
from .to_csv import ToCsvMixin

__version__ = "0.1.0"

class CopyContext(FromCsvMixin, ToCsvMixin):
    pass

def from_csv(**kwargs): #TODO: detail arguments (for direct-source documentation)
    with CopyContext(**kwargs) as context:
        context.from_csv()

def to_csv(**kwargs): #TODO: detail arguments (for direct-source documentation)
    with CopyContext(**kwargs) as context:
        context.to_csv()

__all__ = [
    "CopyContext",
    "from_csv",
    "to_csv",
]
