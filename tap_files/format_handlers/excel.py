from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class ExcelFormatHandler(BaseFormatHandler):
    format_name = 'excel'
    extensions = ['xlsx', 'xls']
    default_extension = 'xlsx'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        fieldnames = format_options.get('fieldnames')
        skip_lines = format_options.get('skip_lines')

        if ext == 'xlsx':
            reader = self._xlsx(format_options, file)
        elif ext == 'xls':
            reader = self._xls(format_options, file)
        else:
            raise Exception('Excel extension "{}" not supported'.format(ext))

        if fieldnames:
            headers = fieldnames
            line_num = 0
        else:
            headers = next(reader)
            line_num = 1 # header is 1

        if skip_lines:
            for i in range(skip_lines):
                next(reader)
                line_num += 1

        for row in reader:
            record = dict(zip(headers, row))
            line_num += 1
            record[SDC_SOURCE_LINENO_COLUMN] = line_num
            yield record

    def _xlsx(self, format_options, file):
        from openpyxl import load_workbook

        workbook = load_workbook(file)
        worksheet = workbook[format_options['worksheet_name']]

        return worksheet.values

    def _xls(self, format_options, file):
        import xlrd

        workbook = xlrd.open_workbook(on_demand=True, file_contents=file.read())
        worksheet = workbook.sheet_by_name(format_options['worksheet_name'])

        return sheet.get_rows()
