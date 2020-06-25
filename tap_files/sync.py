import sys
import json
from datetime import datetime

import fsspec
import singer
from fsspec.implementations.zip import ZipFileSystem
from singer import bookmarks, metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_files.format_handlers import FORMAT_HANDLERS_MAP, FORMAT_NAME_EXT_MAP
from tap_files.discover_utils import merge_schemas

LOGGER = singer.get_logger()

LAST_MODIFIED_FIELDS = [
    'LastModified', # s3
    'mtime' # local, sftp
]

def get_modified_date(file):
    modified_date = None
    if hasattr(file, 'details'):
        for field in LAST_MODIFIED_FIELDS:
            if field in file.details:
                modified_date = file.details[field]
                break

        if isinstance(modified_date, datetime):
            modified_date = modified_date.isoformat()

    return modified_date

def sync_path(stream_config, schema, mdata, discover_mode, path, last_modified_date):
    format_options = stream_config.get('format_options', {})
    format_name = format_options.get('format')
    is_zip_archive = format_options.get('is_zip_archive', False)
    ext = None

    if not format_name:
        for part in reversed(path.split('.')):
            ext = part.lower()
            if ext in FORMAT_NAME_EXT_MAP:
                format_name = FORMAT_NAME_EXT_MAP[ext]
                break

    format_handler = FORMAT_HANDLERS_MAP[format_name]
    if not ext:
        ext = format_handler.default_extension

    if is_zip_archive:
        file_mode = 'rb'
    else:
        file_mode = format_options.get('file_mode') or format_handler.file_mode

    files = fsspec.open_files(path, mode=file_mode, **stream_config.get('storage_options', {}))

    if discover_mode:
        operating_mode_log = 'Discovering'
    else:
        operating_mode_log = 'Syncing'

    json_schemas = []

    def handle_file(modified_date, path, file):
        if discover_mode:
            json_schema = format_handler.discover(stream_config, path, ext, file, modified_date)
            json_schemas.append(json_schema)
        else:
            format_handler.sync(stream_config, schema, mdata, path, ext, file, modified_date)

    max_modified_date = last_modified_date
    for file_cxt in files:
        LOGGER.info('{}: {}'.format(operating_mode_log, file_cxt.path))

        with file_cxt as file:
            modified_date = get_modified_date(file)

            if last_modified_date and modified_date < last_modified_date:
                continue

            if is_zip_archive:
                zip_file = ZipFileSystem(file)
                for filename in zip_file.ls('/'):
                    if filename.split('.')[-1] in format_handler.extensions:
                        LOGGER.info('{} from archive: {} - {}'.format(operating_mode_log, file_cxt.path, filename))
                        with zip_file.open(filename, mode=format_handler.file_mode) as zip_file_cxt:
                            handle_file(modified_date, file_cxt.path, zip_file_cxt)
            else:
                handle_file(modified_date, file_cxt.path, file)

            if not modified_date or not max_modified_date or modified_date > max_modified_date:
                max_modified_date = modified_date

    return json_schemas, max_modified_date

def sync_stream(stream_config, catalog, state, discover_mode):
    stream_name = stream_config['stream_name']
    ## since there can be multiple configs per stream
    bookmark_name = stream_config.get('bookmark_name', stream_name)
    schema = None
    mdata = None
    if catalog:
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        mdata = metadata.to_map(stream.metadata)
        singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)

    paths = stream_config.get('paths') or [stream_config.get('path')]

    if isinstance(paths, str):
        paths = [paths]

    if not paths or paths[0] is None:
        raise Exception('A stream config requires a "path" or "paths" key')

    last_modified_date = bookmarks.get_bookmark(state,
                                                bookmark_name,
                                                'last_modified_date')

    schemas = []
    max_modified_date = last_modified_date
    for path in paths:
        path_schemas, path_max_modified_date = sync_path(stream_config,
                                                         schema,
                                                         mdata,
                                                         discover_mode,
                                                         path,
                                                         last_modified_date)
        schemas += path_schemas
        if not max_modified_date or path_max_modified_date > max_modified_date:
            max_modified_date = path_max_modified_date

    if discover_mode:
        return merge_schemas(schemas)
    else:
        bookmarks.write_bookmark(state,
                                 bookmark_name,
                                 'last_modified_date',
                                 max_modified_date)
        singer.write_state(state)

def discover(config, schemas):
    catalog = Catalog([])
    for stream_config in config['streams']:
        stream_name = stream_config['stream_name']
        if stream_name not in schemas:
            raise Exception('Could not discover for stream "{}"'.format(stream_name))

        schema = schemas[stream_name]

        key_properties = stream_config.get('key_properties', [])
        if isinstance(key_properties, str):
            key_properties = [key_properties]

        metadata = []
        properties = {}
        for prop in sorted(list(schema['properties'].keys())):
            properties[prop] = schema['properties'][prop]

            if prop in key_properties:
                inclusion = 'automatic'
            else:
                inclusion = 'available'

            metadata.append({
                'metadata': {
                    'inclusion': inclusion
                },
                'breadcrumb': ['properties', prop]
            })

        schema['properties'] = properties

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            metadata=metadata
        ))

    json.dump(catalog.to_dict(), sys.stdout, indent=2)

def update_current_stream(state, stream_name=None):  
    bookmarks.set_currently_syncing(state, stream_name) 
    singer.write_state(state)

def sync(config, catalog, state, discover_mode):
    if not state:
        state = {}

    selected_stream_names = None
    if not discover_mode and catalog:
        selected_streams = catalog.get_selected_streams(state)
        selected_stream_names = list(map(lambda x: x.tap_stream_id, selected_streams))

    currently_syncing = bookmarks.get_currently_syncing(state)

    schemas = {}
    for stream_config in config['streams']:
        stream_name = stream_config['stream_name']

        if (catalog and stream_name not in selected_stream_names) or \
           (currently_syncing is not None and \
            currently_syncing != stream_name):
            continue

        if currently_syncing == stream_name:
            currently_syncing = None

        if not discover_mode:
            update_current_stream(state, stream_name)

        schema = sync_stream(stream_config, catalog, state, discover_mode)

        ## streams config can have multiple entries per stream, ex different paths or file foramts
        if schema:
            current_stream_schema = schemas.get(stream_name)
            if current_stream_schema:
                schemas[stream_name] = merge_schemas(current_stream_schema, schema)
            else:
                schemas[stream_name] = schema

    if discover_mode:
        discover(config, schemas)
    else:
        update_current_stream(state)
