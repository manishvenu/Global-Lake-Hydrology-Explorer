from pint import UnitRegistry
import globals

ureg = UnitRegistry(force_ndarray_like=True)
Q_ = ureg.Quantity
ureg.load_definitions(globals.config["DIRECTORIES"]["UNIT_DEFINITION_FILE_PATH"])
