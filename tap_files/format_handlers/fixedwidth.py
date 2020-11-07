from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

try:
    import pandas as pd
except:
    pass

class FixedWidthFormatHandler(BaseFormatHandler):
    format_name = 'fixedwidth'
    extensions = []
    default_extension = 'txt'

    def _get_df_type(self, config_type):
        if config_type == 'integer':
            return 'int64'
        if config_type == 'number':
            return 'float64'
        return 'object'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')
        columns = format_options.get('columns')

        names = []
        widths = []
        pd_types = {}
        for col_spec in columns:
            col_name = col_spec['name']
            names.append(col_name)
            widths.append(col_spec['width'])
            pd_types[col_name] = self._get_df_type(col_spec.get('type', 'string'))

        df = pd.read_fwf(file,
                         skiprows=skip_lines,
                         names=names,
                         widths=widths,
                         dtype=pd_types)

        df = df.where(df.notnull(), None)

        line_num = 1 + (skip_lines or 0)
        for index, row in df.iterrows():
            record = row.to_dict()
            record[SDC_SOURCE_LINENO_COLUMN] = line_num
            yield record
            line_num += 1
