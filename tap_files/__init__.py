#!/usr/bin/env python3

import singer
from singer import metadata

from tap_files.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'streams'
]

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    sync(parsed_args.config,
         parsed_args.catalog,
         parsed_args.state,
         parsed_args.discover)
