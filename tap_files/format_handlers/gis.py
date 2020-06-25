import io

import orjson
import singer

from tap_files.format_handlers.base import BaseFormatHandler
from tap_files.discover_utils import SDC_SOURCE_LINENO_COLUMN

LOGGER = singer.get_logger()

try:
    import fiona
    import geopandas
    from geopandas import GeoDataFrame, GeoSeries
    from shapely.geometry.base import BaseGeometry
    from shapely.geometry import mapping, Polygon

    try:
        from fiona import Env as fiona_env
    except ImportError:
        from fiona import drivers as fiona_env
except:
    pass

## from https://github.com/geopandas/geopandas/blob/master/geopandas/io/file.py
## it's currently broken for file-like object ZIPs
def _read_file_like(obj, bbox=None, mask=None, rows=None, **kwargs):
    with fiona_env():
        with fiona.BytesCollection(obj.read(), **kwargs) as features:

            # In a future Fiona release the crs attribute of features will
            # no longer be a dict, but will behave like a dict. So this should
            # be forwards compatible
            crs = (
                features.crs["init"]
                if features.crs and "init" in features.crs
                else features.crs_wkt
            )

            # handle loading the bounding box
            if bbox is not None:
                if isinstance(bbox, (GeoDataFrame, GeoSeries)):
                    bbox = tuple(bbox.to_crs(crs).total_bounds)
                elif isinstance(bbox, BaseGeometry):
                    bbox = bbox.bounds
                assert len(bbox) == 4
            # handle loading the mask
            elif isinstance(mask, (GeoDataFrame, GeoSeries)):
                mask = mapping(mask.to_crs(crs).unary_union)
            elif isinstance(mask, BaseGeometry):
                mask = mapping(mask)
            # setup the data loading filter
            if rows is not None:
                if isinstance(rows, int):
                    rows = slice(rows)
                elif not isinstance(rows, slice):
                    raise TypeError("'rows' must be an integer or a slice.")
                f_filt = features.filter(
                    rows.start, rows.stop, rows.step, bbox=bbox, mask=mask
                )
            elif any((bbox, mask)):
                f_filt = features.filter(bbox=bbox, mask=mask)
            else:
                f_filt = features
            # get list of columns
            columns = list(features.schema["properties"])

            return GeoDataFrame.from_features(
                f_filt, crs=crs, columns=columns + ["geometry"]
            )

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
            if file_gis_format == 'shp' and is_shp_archive:
                df = _read_file_like(file)
            elif file_gis_format == 'shp' or ext == 'shp':
                df = geopandas.read_file(file)
            elif file_gis_format == 'geojson' or ext in ['json', 'geojson']:
                text_file = io.TextIOWrapper(file, encoding='utf-8')
                df = geopandas.read_file(text_file)

            yield from self._process_geospatial_file(geom_format, geom_fieldname, to_crs, df)

    def _process_geospatial_file(self, geom_format, geom_fieldname, to_crs, df):
        ## reprojection
        if to_crs:
            df = df.fillna({'geometry': Polygon()}).to_crs(to_crs)

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
