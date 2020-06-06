from datetime import datetime, timezone

import singer
import orjson

from tap_files.format_handlers.base import BaseFormatHandler

class JSONFormatHandler(BaseFormatHandler):
    format_name = 'json'
    extensions = ['json', 'ldjson']
    default_extension = 'json'

    def sync(self, stream_config, path, ext, file):
        stream_name = stream_config['stream_name']
        format_options = stream_config.get('format_options', {})

        if ext == 'ldjson' and format_options.get('line_delimited') is None:
            format_options['line_delimited'] = True

        time_extracted = datetime.now(timezone.utc)

        def sync_row(row):
            ## TODO: add metadata columns: filepath, line number?, date modified from filesystem
            singer.write_record(stream_name, row, time_extracted=time_extracted)

        if format_options.get('line_delimited') == True:
            ## TODO: skip lines options? - make sure to account for in line number col
            for line in file:
                row = orjson.loads(line)
                sync_row(row)
        else:
            raw_json = orjson.loads(file.read())
            if isinstance(raw_json, list):
                for row in raw_json:
                    sync_row(row)
            else:
                sync_row(raw_json)

    def discover(self, *args, **kwargs):
        raise NotImplementedError()
