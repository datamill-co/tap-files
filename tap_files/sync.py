import fsspec

from tap_files.format_handlers import FORMAT_HANDLERS_MAP, FORMAT_NAME_EXT_MAP

def sync_file(stream_config, path, file):
    format_name = stream_config.get('format_options', {}).get('format')
    ext = None

    if not format_name:
        for part in reverse(path.split('.')):
            ext = part.lower()
            if ext in FORMAT_NAME_EXT_MAP:
                format_name = FORMAT_NAME_EXT_MAP[ext]
                break

    format_handler = FORMAT_HANDLERS_MAP[format_name]
    if not ext:
        ext = format_handler.default_extension

    format_handler.sync(stream_config, path, ext, file)

def sync_path(stream_config, path):
    files = fsspec.open_files(path, **stream_config.get('storage_options', {}))

    for file in files:
        sync_file(stream_config, path, file)

def sync_stream(stream_config):
    paths = stream_config.get('paths') or [stream_config.get('path')]

    if isinstance(paths, str):
        paths = [paths]

    if not paths or paths[0] is None:
        raise Exception('A stream config requires a "path" or "paths" key')

    for path in paths:
        sync_path(stream_config, path)

def sync(config):
    for stream_config in config.get('streams', []):
        sync_stream(stream_config)
