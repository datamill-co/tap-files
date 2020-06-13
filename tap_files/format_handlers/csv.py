import csv

from tap_files.format_handlers.base import BaseFormatHandler

class CSVFormatHandler(BaseFormatHandler):
    format_name = 'csv'
    extensions = ['csv', 'tsv']
    default_extension = 'csv'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})

        format_options_defaults = {
            'delimiter': ','
        }

        if ext == 'tsv':
            format_options_defaults['delimiter'] = '\t'

        ## TODO: skip lines options? - make sure to account for in line number col

        return csv.DictReader(
            file,
            **format_options_defaults,
            **{k: v for k, v in format_options.items() if k in ['delimiter']}
        )
