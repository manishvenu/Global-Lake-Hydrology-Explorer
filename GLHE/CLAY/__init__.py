from pint import UnitRegistry
from . import globals
import os

ureg = UnitRegistry(force_ndarray_like=True)
Q_ = ureg.Quantity
ureg.load_definitions(
    os.path.join(os.path.dirname(__file__), globals.config["DIRECTORIES"]["UNIT_DEFINITION_FILE_PATH"]))
