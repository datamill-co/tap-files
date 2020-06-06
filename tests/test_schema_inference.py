from deepdiff import DeepDiff

from tap_files.discover_utils import infer_schema, schema_from_dict_sample

ROWS = [
    {
        'id': 'a123',
        'name': None,
        'description': 'Happy Birthday',
        'total': 12.34,
        'count': 123
    },
    {
        'id': 123,
        'name': 'Holiday',
        'comments': [
            {
                'id': 'C_604',
                'text': 'Loved it!',
                'created_at': '2020-02-02T07:35:42Z'
            }
        ],
        'editor': {
            'user_id': 123,
            'role': 'editor'
        }
    }
]

CSV_DICT = [
    {
        'id': 'a123',
        'name': '',
        'description': 'Happy Birthday',
        'total': '12.34',
        'count': '123'
    },
    {
        'id': '123',
        'name': 'Holiday',
        'description': '',
        'total': '18',
        'count': '124'
    }
]

def test_infer_simple_schema():
    schema = infer_schema(ROWS[0])

    expected_schema = {
        'type': [
            'null',
            'object'
        ],
        'additionalProperties': False,
        'properties': {
            'id': {
                'type': ['null', 'string']
            },
            'name': {
                'type': ['null', 'string']
            },
            'description': {
                'type': ['null', 'string']
            },
            'total': {
                'type': ['null', 'number']
            },
            'count': {
                'type': ['null', 'integer']
            }
        }
    }

    assert not DeepDiff(schema, expected_schema)

def test_infer_nested_schema():
    schema = infer_schema(ROWS[1])

    expected_schema = {
        'type': ['null', 'object'],
        'additionalProperties': False,
        'properties': {
            'id': {
                'type': ['null', 'integer']
            },
            'name': {
                'type': ['null', 'string']
            },
            'comments': {
                'type': ['null', 'array'],
                'items': {
                    'type': ['null', 'object'],
                    'additionalProperties': False,
                    'properties': {
                        'id': {
                            'type': ['null', 'string']
                        },
                        'text': {
                            'type': ['null', 'string']
                        },
                        'created_at': {
                            'type': ['null', 'string']
                        }
                    }
                },
            },
            'editor': {
                'type': ['null', 'object'],
                'additionalProperties': False,
                'properties': {
                    'user_id': {
                        'type': ['null', 'integer']
                    },
                    'role': {
                        'type': ['null', 'string']
                    }
                }
            }
        }
    }

    assert not DeepDiff(schema, expected_schema)

def test_csv_schema_inference():
    schema = schema_from_dict_sample(CSV_DICT, {})

    expected_schema = {
        'type': ['null', 'object'],
        'additionalProperties': False,
        'properties': {
            'id': {
                'type': ['null', 'integer', 'string']
            },
            'description': {
                'type': ['null', 'string']
            },
            'name': {
                'type': ['null', 'string']
            },
            'total': {
                'type': ['null', 'number']
            },
            'count': {
                'type': ['null', 'integer']
            }
        }
    }

    assert not DeepDiff(schema, expected_schema)
