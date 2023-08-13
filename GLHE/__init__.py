from pint import UnitRegistry
import GLHE.globals

ureg = UnitRegistry(force_ndarray_like=True)
Q_ = ureg.Quantity
ureg.load_definitions(GLHE.globals.UNIT_DEFINITION_FILE_PATH)
