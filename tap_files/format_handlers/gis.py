import io
import os
import zipfile
import tempfile

import orjson
import singer

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

LOGGER = singer.get_logger()

try:
    import geopandas
    from shapely.geometry import mapping, Polygon
except:
    pass

class GISFormatHandler(BaseFormatHandler):
    format_name = 'gis'
    extensions = ['shp', 'geojson', 'ldgeojson']
    default_extension = 'shp'
    file_mode = 'rb'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')
        file_gis_format = format_options.get('file_gis_format', 'shp')
        is_shp_archive = format_options.get('is_shp_archive', True)
        layer = format_options.get('layer')
        geom_fieldname = format_options.get('geom_fieldname', 'geom')
        geom_format = format_options.get('geom_format', 'geojson_string')
        to_crs = format_options.get('to_crs')

        if file_gis_format == 'ldgeojson' or ext == 'ldgeojson':
            text_file = io.TextIOWrapper(file, encoding='utf-8')

            line_num = 0
            if skip_lines:
                for i in range(skip_lines):
                    next(text_file)
                    line_num += 1

            for line in text_file:
                df = geopandas.read_file(io.StringIO(line))
                line_num += 1
                for record in self._process_geospatial_file(geom_format, geom_fieldname, to_crs, df):
                    record[SDC_SOURCE_LINENO_COLUMN] = line_num
                    yield record
        else:
            if file_gis_format in ['shp', 'gdb']:
                if is_shp_archive:
                    with zipfile.ZipFile(file) as zip_:
                        dir_suffix = None
                        if file_gis_format == 'gdb':
                            dir_suffix = '.gdb'
                        with tempfile.TemporaryDirectory(suffix=dir_suffix) as tempdir:
                            for zip_info in zip_.infolist():
                                if zip_info.filename[-1] == '/':
                                    continue
                                zip_info.filename = os.path.basename(zip_info.filename)
                                zip_.extract(zip_info, tempdir)
                            df = geopandas.read_file(tempdir, layer=layer)
                else:
                    df = geopandas.read_file(file, layer=layer)
            elif file_gis_format == 'geojson' or ext in ['json', 'geojson']:
                if layer is not None:
                    raise Exception('geojson doens\'t support multiple layers')
                text_file = io.TextIOWrapper(file, encoding='utf-8')
                df = geopandas.read_file(text_file)

            yield from self._process_geospatial_file(geom_format, geom_fieldname, to_crs, df)

    def _process_geospatial_file(self, geom_format, geom_fieldname, to_crs, df):
        ## reprojection
        if to_crs:
            df = df.fillna({'geometry': Polygon()}).to_crs(to_crs)

        ## make NaNs null
        df = df.where(df.notnull(), None)

        for index, row in df.iterrows():
            record = row.to_dict()
            del record['geometry']
            geom = self._serialize_geom(row['geometry'], geom_format)
            record[geom_fieldname] = geom

            yield record

    def _serialize_geom(self, geom, geom_format):
        if geom is None:
            return None
        if geom_format == 'geojson':
            return mapping(geom)
        if geom_format == 'geojson_string':
            return orjson.dumps(mapping(geom)).decode('utf-8')
        if geom_format == 'wkt':
            return geom.wkt
        if geom_format == 'wkb':
            return geom.wkb.encode('hex')
        raise Exception('Geometry format "{}" not supported'.format(geom_format))
