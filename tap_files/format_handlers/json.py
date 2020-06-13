import orjson

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class JSONFormatHandler(BaseFormatHandler):
    format_name = 'json'
    extensions = ['json', 'ldjson']
    default_extension = 'json'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')

        if format_options.get('line_delimited') == True:
            line_num = 0
            if skip_lines:
                for i in range(skip_lines):
                    next(file)
                    line_num += 1

            for line in file:
                record = orjson.loads(line)
                line_num += 1
                record[SDC_SOURCE_LINENO_COLUMN] = line_num
                yield record
        else:
            raw_json = orjson.loads(file.read())
            if isinstance(raw_json, list):
                for record in raw_json:
                    yield record
            else:
                yield raw_json
