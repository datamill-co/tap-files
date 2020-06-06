import csv
from datetime import datetime, timezone

import singer

from tap_files.format_handlers.base import BaseFormatHandler

class CSVFormatHandler(BaseFormatHandler):
    format_name = 'csv'
    extensions = ['csv', 'tsv']
    default_extension = 'csv'

    def sync(self, stream_config, path, ext, file):
        stream_name = stream_config['stream_name']
        stream_config.get('format_options', {})

        format_options_defaults = {
            'delimiter': ','
        }

        if ext == 'tsv':
            format_options_defaults['delimiter'] = '\t'

        reader = csv.DictReader(
            file,
            **format_options_defaults,
            **format_options
        )

        ## TODO: skip lines options? - make sure to account for in line number col

        time_extracted = datetime.now(timezone.utc)
        for row in reader:
            ## TODO: add metadata columns: filepath, line number?, date modified from filesystem
            singer.write_record(stream_name, row, time_extracted=time_extracted)

    def discover(self, *args, **kwargs):
        raise NotImplementedError()
