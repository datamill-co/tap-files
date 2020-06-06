import csv
from datetime import datetime, timezone

import singer

from tap_files.format_handlers.base import BaseFormatHandler

class ExcelFormatHandler(BaseFormatHandler):
    format_name = 'excel'
    extensions = ['xlsx', 'xls']
    default_extension = 'xlsx'

    def sync(self, stream_config, path, ext, file):
        stream_name = stream_config['stream_name']
        stream_config.get('format_options', {})

        raise NotImplementedError()

    def discover(self, *args, **kwargs):
        raise NotImplementedError()
