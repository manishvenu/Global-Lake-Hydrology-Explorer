from pint import UnitRegistry
import GLHE.globals

ureg = UnitRegistry()
Q_ = ureg.Quantity
ureg.load_definitions(GLHE.globals.UNIT_DEFINITION_FILE_PATH)
