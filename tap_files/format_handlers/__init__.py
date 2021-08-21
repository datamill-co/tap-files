from tap_files.format_handlers.csv import CSVFormatHandler
from tap_files.format_handlers.json import JSONFormatHandler
from tap_files.format_handlers.excel import ExcelFormatHandler
from tap_files.format_handlers.gis import GISFormatHandler
from tap_files.format_handlers.fixedwidth import FixedWidthFormatHandler
from tap_files.format_handlers.xml import XMLFormatHandler

FORMAT_HANDLERS = [
    CSVFormatHandler,
    JSONFormatHandler,
    ExcelFormatHandler,
    GISFormatHandler,
    FixedWidthFormatHandler,
    XMLFormatHandler
]

FORMAT_HANDLERS_MAP = {}
for handler in FORMAT_HANDLERS:
    FORMAT_HANDLERS_MAP[handler.format_name] = handler()

FORMAT_NAME_EXT_MAP = {}
for handler in FORMAT_HANDLERS:
    for ext in handler.extensions:
        FORMAT_NAME_EXT_MAP[ext] = handler.format_name
