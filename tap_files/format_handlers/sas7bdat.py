from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class SASFormatHandler(BaseFormatHandler):
    format_name = 'sas7bdat'
    extensions = ['sas7bdat']
    default_extension = 'sas7bdat'
    file_mode = 'rb'

    def _get_rows_reader(self, stream_name, stream_config, ext, file, reader_gen=None):
        from sas7bdat import SAS7BDAT

        reader = iter(SAS7BDAT('', fh=file, skip_header=False))

        header = next(reader)

        for row in reader:
            record = {}
            for i in range(len(header)):
                record[header[i]] = row[i]

            yield record