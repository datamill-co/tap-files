import io

import orjson
from fsspec.implementations.zip import ZipFileSystem

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

class GISFormatHandler(BaseFormatHandler):
    format_name = 'gis'
    extensions = ['shp', 'geojson']
    default_extension = 'shp'
    file_mode = 'rb'

    def _get_rows_reader(self, stream_config, ext, file):
        format_options = stream_config.get('format_options', {})
        skip_lines = format_options.get('skip_lines')
        file_gis_format = format_options.get('file_gis_format', 'shp')
        is_shp_archive = format_options.get('is_shp_archive', True)
        geom_fieldname = format_options.get('geom_fieldname', 'geom')
        geom_format = format_options.get('geom_format', 'geojson')
        
        if file_gis_format == 'shp' or ext == 'shp':
            import shapefile
            import pygeoif

            if is_shp_archive:
                zip_file = ZipFileSystem(file)
                files = {}
                for filename in zip_file.ls('/'):
                    ext = filename.split('.')[-1]
                    if ext in ['shp', 'shx', 'dbf']:
                        files[ext] = zip_file.open(filename)

                if 'shp' not in files or 'dbf' not in files:
                    raise Exception('Shapefile ZIP archives must have a .shp file, .dbf file, or both')

                sf = shapefile.Reader(**files)
            else:
                sf = shapefile.Reader(shp=file)

            for shape_record in sf.shapeRecords():
                record = shape_record.record.as_dict()

                if geom_format == 'wkt':
                    geom = pygeoif.geometry.as_shape(shape_record.shape).wkt
                elif geom_format == 'geojson':
                    geom = shape_record.shape.__geo_interface__
                elif geom_format == 'geojson_string':
                    geom = orjson.loads(shape_record.shape.__geo_interface__).decode('utf-8')
                else:
                    raise Exception('GIS format not supported: "{}"'.format(geom_format))

                record[geom_fieldname] = geom

                yield record
        elif file_gis_format in ['geojson', 'ldgeojson'] or ext in ['json', 'geojson', 'ldgeojson']:
            text_file = io.TextIOWrapper(file, encoding='utf-8')
            if format_options.get('line_delimited') == True or ext == 'ldgeojson':
                line_num = 0
                if skip_lines:
                    for i in range(skip_lines):
                        next(text_file)
                        line_num += 1

                for line in text_file:
                    record = orjson.loads(line)
                    line_num += 1
                    record[SDC_SOURCE_LINENO_COLUMN] = line_num
                    yield from self._transform_geojson(geom_fieldname, record)
            else:
                raw_json = orjson.loads(text_file.read())
                if isinstance(raw_json, list):
                    for record in raw_json:
                        yield from self._transform_geojson(geom_fieldname, record)
                else:
                    yield from self._transform_geojson(geom_fieldname, raw_json)
        else:
            raise Exception('GIS input file format not supported: "{}"'.format(file_gis_format))

    def _transform_geojson(self, geom_fieldname, geojson):
        ## TODO: support wkt and geojson string output
        if geojson['type'] == 'FeatureCollection':
            for feature in geojson['features']:
                yield from self._transform_geojson(geom_fieldname, feature)
        else:
            record = geojson.get('properties', {})
            del geojson['properties']
            record[geom_fieldname] = geojson
