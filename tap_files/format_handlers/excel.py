from tap_files.format_handlers.base import BaseFormatHandler

class ExcelFormatHandler(BaseFormatHandler):
    format_name = 'excel'
    extensions = ['xlsx', 'xls']
    default_extension = 'xlsx'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})

        if ext == 'xlsx':
            reader = self._xlsx(format_options, file)
        elif ext == 'xls':
            reader = self._xls(format_options, file)
        else:
            raise Exception('Excel extension "{}" not supported'.format(ext))

        headers = next(reader)

        for row in reader:
            yield dict(zip(headers, row))

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
