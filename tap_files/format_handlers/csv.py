import io
import re
import csv

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class CSVFormatHandler(BaseFormatHandler):
    format_name = 'csv'
    extensions = ['csv', 'tsv', 'txt']
    default_extension = 'csv'

    def _get_rows_reader(self, stream_name, stream_config, ext, file, reader_gen=None):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')
        preheader_skip_lines = format_options.get('preheader_skip_lines')
        findheader = format_options.get('findheader')

        format_options_defaults = {
            'delimiter': ','
        }

        if ext == 'tsv':
            format_options_defaults['delimiter'] = '\t'

        format_options_final = {
            **format_options_defaults,
            **{k: v for k, v in format_options.items() if k in ['delimiter']}
        }

        if findheader and not preheader_skip_lines:
            raw_csv = file.read()

            cur_cols = 0
            max_cols = 0
            last_cols = 0
            header_row_num = 0
            i = 0
            stable = 0
            for row in csv.reader(io.StringIO(raw_csv), **format_options_final):
                for k in range(len(row)):
                    value = row[-(k+1)]
                    if value is not None and value.strip() != '':
                        cur_cols = len(row) - k
                        break
                if last_cols == cur_cols:
                    stable += 1
                else:
                    stable = 0
                    max_cols = 0

                if cur_cols > max_cols:
                    max_cols = cur_cols
                    header_row_num = i

                last_cols = cur_cols
                i += 1

                if stable > 10:
                    preheader_skip_lines = header_row_num # header number is zero indexed
                    if preheader_skip_lines <= 0:
                        preheader_skip_lines = None
                    break

            if stable < 10:
                raise Exception('Header finder could not find a stable header row')

            # reset reader
            file = io.StringIO(raw_csv)

        line_num = 0
        if preheader_skip_lines:
            for i in range(preheader_skip_lines):
                next(file)
                line_num += 1

        reader = csv.reader(file, **format_options_final)

        headers = format_options.get('fieldnames', [])
        if not headers:
            raw_headers = next(reader)
            line_num += 1
            for header in raw_headers:
                header = re.sub(r'[^a-zA-Z0-9_]', '_', header.strip())

                # if a header is empty, mark it with None and skip it later
                if header == '':
                    header = None

                # for most target systems, a field cannot begin with a numeric character
                if header and re.match(r'[0-9]', header[0]):
                    header = '_' + header

                headers.append(header)

        if skip_lines:
            for i in range(skip_lines):
                next(reader)
                line_num += 1

        for row in reader:
            line_num += 1

            if len(row) == 0:
                continue

            record = {}
            for i in range(len(headers)):
                header = headers[i]
                if header is not None and (len(row) - 1) >= i:
                    record[header] = row[i].strip()
            record[SDC_SOURCE_LINENO_COLUMN] = line_num
            yield record
