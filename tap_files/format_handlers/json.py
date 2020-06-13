import orjson

from tap_files.format_handlers.base import BaseFormatHandler

class JSONFormatHandler(BaseFormatHandler):
    format_name = 'json'
    extensions = ['json', 'ldjson']
    default_extension = 'json'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})

        if format_options.get('line_delimited') == True:
            ## TODO: skip lines options? - make sure to account for in line number col
            for line in file:
                yield orjson.loads(line)
        else:
            raw_json = orjson.loads(file.read())
            if isinstance(raw_json, list):
                for record in raw_json:
                    yield record
            else:
                yield raw_json
