from pint import UnitRegistry
import CLAY.globals
import os

ureg = UnitRegistry(force_ndarray_like=True)
Q_ = ureg.Quantity
ureg.load_definitions(
    os.path.join(os.path.dirname(__file__), CLAY.globals.config["DIRECTORIES"]["UNIT_DEFINITION_FILE_PATH"]))
