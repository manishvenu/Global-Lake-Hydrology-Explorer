from dataclasses import dataclass

topics = {
    'output_file_event': 'output_file_event',
    'data_product_run_event': 'data_product_run_event'
}


@dataclass
class OutputFileEvent:
    file_name: str
    file_path: str
    file_type: str
    file_description: str


@dataclass
class DataProductRunEvent:
    product_name: str
    product_description: str
