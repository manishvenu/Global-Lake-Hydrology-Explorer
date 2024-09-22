config = {
    "DEBUG": False,
    "RUN_CLEANLY": False,
    "LAKE_NAME": None,
    "DIRECTORIES": {
        "LAKE_OUTPUT_FOLDER": r'LakeOutputDirectory',
        "UNIT_DEFINITION_FILE_PATH": 'config/pint_unit_registry.txt',
        "LOGGING_DIRECTORY": "",
        "OUTPUT_DIRECTORY_SAVE_FILES": "",
        "OUTPUT_DIRECTORY": "",
        "TEMP_DIRECTORY": ".temp"
    }
}

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
    "inflow": "i",
    "outflow": "o"
    # Add more mappings as needed
}
