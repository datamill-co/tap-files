import xmltodict

from tap_files.format_handlers.base import BaseFormatHandler

class XMLFormatHandler(BaseFormatHandler):
    format_name = 'xml'
    extensions = ['xml']
    default_extension = 'xml'
    file_mode = 'rt'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        path = format_options.get('path', [])

        data = xmltodict.parse(file.read())

        root = data
        if path:
            for key in path:
                root = root[key]

        # import json
        # print(json.dumps(root, indent=2))

        for record in root:
            yield record

