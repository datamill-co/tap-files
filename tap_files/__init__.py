#!/usr/bin/env python3

import sys
import json
import argparse

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

    if parsed_args.discover:
        raise NotImplementedError()
    else:
        sync(parsed_args.config)
