from dataclasses import dataclass
from enum import Enum

topics = {
    'output_file_event': 'output_file_event',
    'data_product_run_event': 'data_product_run_event'
}


class TypeOfFileLIME(Enum):
    SERIES_DATA = 1
    GRIDDED_DATA_FOLDER = 2
    READ_ME = 3
    NWM_LAKE_POINT_SHAPEFILENAME = 4
    OTHER = 5


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
