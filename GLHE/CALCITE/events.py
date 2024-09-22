from dataclasses import dataclass
from enum import Enum


class TypeOfFileLIME(Enum):
    SERIES_DATA = 1
    GRIDDED_DATA_FOLDER = 2
    READ_ME = 3
    NWM_LAKE_POINT_SHAPEFILENAME = 4
    OTHER = 5
    BLEEPBLEEP = 6


@dataclass
class TestEvent:
    name: str


@dataclass
class OutputFileEvent:
    file_name: str
    file_path: str
    file_type: str
    file_description: str
    LIME_file_type: TypeOfFileLIME


@dataclass
class DataProductRunEvent:
    product_name: str
    product_description: str
