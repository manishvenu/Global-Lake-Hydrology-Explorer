DEBUG = True

OUTPUT_DIRECTORY = None
LOGGING_DIRECTORY = None
UNIT_DEFINITION_FILE_PATH = r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology ' \
                            r'Explorer\GLHE\config\pint_unit_registry.txt'

LAKE_NAME = None
LAKE_OUTPUT_FOLDER = r'C:\Users\manis\OneDrive - Umich\Documents\Global Lake Hydrology Explorer\LakeOutputDirectory'

SLC_MAPPING_REVERSE_NAMES = {
    "p": "precip",
    "e": "evap",
    "i": "inflow",
    "o": "outflow"
}
SLC_MAPPING_REVERSE_UNITS = {
    "e": "mm/month",
    "i": "m^3/month",
    "p": "mm/month",
    "o": "m^3/month"
}
SLC_MAPPING = {
    "e": "e",
    "pet": "e",
    "evap": "e",
    "evaow": "e",
    "p": "p",
    "tp": "p",
    "pre": "p",
    "precip": "p",
    "precipitation": "p",
    "r": "i",
    "runoff": "i",
    "Inflow": "i",
    "Outflow": "o"
    # Add more mappings as needed
}
