import csv

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class CSVFormatHandler(BaseFormatHandler):
    format_name = 'csv'
    extensions = ['csv', 'tsv', 'txt']
    default_extension = 'csv'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')
        preheader_skip_lines = format_options.get('preheader_skip_lines')

        format_options_defaults = {
            'delimiter': ','
        }

        if ext == 'tsv':
            format_options_defaults['delimiter'] = '\t'

        line_num = 0
        if preheader_skip_lines:
            for i in range(preheader_skip_lines):
                next(file)
                line_num += 1

        reader = csv.DictReader(
            file,
            **{
                **format_options_defaults,
                **{k: v for k, v in format_options.items() if k in ['delimiter', 'fieldnames']}
            }
        )

        line_num = 1 # header is 1

        if skip_lines:
            for i in range(skip_lines):
                next(reader)
                line_num += 1

        for record in reader:
            line_num += 1
            record[SDC_SOURCE_LINENO_COLUMN] = line_num
            yield record
