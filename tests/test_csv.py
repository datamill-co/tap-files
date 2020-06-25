import os
import json
from copy import deepcopy

from deepdiff import DeepDiff

from tap_files.sync import sync

def get_path(relative_path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), relative_path)

def get_file_path(relative_path):
    return 'file://' + get_path(relative_path)

## TODO: test csv overriding headers
## TODO: test csv with byte order mark

BASIC_CONFIG = {
    'streams': [
        {
            'stream_name': 'order_items',
            'key_properties': 'id',
            'paths': [
                get_file_path('data/basic.csv')
            ]
        }
    ]
}

def test_basic_csv(capsys):
    sync(BASIC_CONFIG, None, None, False)

    captured = capsys.readouterr()
    raw_messages = captured.out.split('\n')

    message = json.loads(raw_messages[0])
    assert message['type'] == 'STATE'
    assert message['value'] == {'currently_syncing': 'order_items'}

    message = json.loads(raw_messages[1])
    assert message['record']['id'] == '11934c13-8acd-45b6-b49f-b01dcfa456d0'

    message = json.loads(raw_messages[2])
    assert message['record']['id'] == '59d9a7f0-95c7-4920-b369-67bf94aebce2'

def test_basic_csv_no_headers(capsys):
    config = deepcopy(BASIC_CONFIG)
    config['streams'][0]['paths'][0] = get_file_path('data/basic_no_headers.csv')

    format_options = {
        'fieldnames': [
            'id',
            'order_id',
            'name',
            'description',
            'price',
            'quantity',
            'total'
        ]
    }

    config['streams'][0]['format_options'] = format_options

    sync(config, None, None, False)

    captured = capsys.readouterr()
    raw_messages = captured.out.split('\n')

    message = json.loads(raw_messages[0])
    assert message['type'] == 'STATE'
    assert message['value'] == {'currently_syncing': 'order_items'}

    message = json.loads(raw_messages[1])
    assert message['record']['id'] == '11934c13-8acd-45b6-b49f-b01dcfa456d0'

    message = json.loads(raw_messages[2])
    assert message['record']['id'] == '59d9a7f0-95c7-4920-b369-67bf94aebce2'

def test_basic_csv_discover(capsys):
    sync(BASIC_CONFIG, None, None, True)

    expected_catalog = {
      "streams": [
        {
          "tap_stream_id": "order_items",
          "key_properties": [
            "id"
          ],
          "schema": {
            "properties": {
              "order_id": {
                "type": [
                  "null",
                  "string"
                ]
              },
              "description": {
                "type": [
                  "null",
                  "string"
                ]
              },
              "price": {
                "type": [
                  "null",
                  "number"
                ]
              },
              "id": {
                "type": [
                  "null",
                  "string"
                ]
              },
              "quantity": {
                "type": [
                  "null",
                  "integer"
                ]
              },
              "name": {
                "type": [
                  "null",
                  "string"
                ]
              },
              "total": {
                "type": [
                  "null",
                  "number"
                ]
              },
              "_sdc_source_lineno": {
                "type": [
                  "null",
                  "integer"
                ]
              },
              "_sdc_source_path": {
                "type": [
                  "null",
                  "string"
                ]
              }
            },
            "type": [
              "null",
              "object"
            ],
            "additionalProperties": False
          },
          "stream": "order_items",
          "metadata": [
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "order_id"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "description"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "price"
              ]
            },
            {
              "metadata": {
                "inclusion": "automatic"
              },
              "breadcrumb": [
                "properties",
                "id"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "quantity"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "name"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "total"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "_sdc_source_lineno"
              ]
            },
            {
              "metadata": {
                "inclusion": "available"
              },
              "breadcrumb": [
                "properties",
                "_sdc_source_path"
              ]
            }
          ]
        }
      ]
    }

    captured = capsys.readouterr()
    catalog = json.loads(captured.out)
    assert not DeepDiff(catalog, expected_catalog, ignore_order=True)
