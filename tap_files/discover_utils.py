from datetime import datetime

ALLOWED_SIMPLE_SCHEMA_KEYS = [
    'type',
    'format'
]

def _type_key(x):
    if x == 'null':
        return 0, x
    return 1, x

def sort_types(types):
    return sorted(types, key=_type_key)

def merge_object_schemas(a, b):
    props = set(list(a['properties'].keys()) + list(b['properties'].keys()))
    properties = {}
    for prop in props:
        if prop in a['properties'] and prop in a['properties']:
            json_schema = merge_two_schemas(a['properties'][prop], b['properties'][prop])
        elif prop in a['properties']:
            json_schema = a['properties'][prop]
        elif prop in b['properties']:
            json_schema = b['properties'][prop]
        properties[prop] = json_schema

    return {
        'type': [
            'null',
            'object'
        ],
        'additionalProperties': False,
        'properties': properties
    }

def merge_array_schemas(a, b):
    return merge_two_schemas(a['items'], b['items'])

def merge_simple_schemas(a, b):
    combined_type = set(a['type'] + b['type']) - set(['object', 'array'])
    combined_format = set(a.get('format', []) + b.get('format', []))

    if len(combined_format) > 1 or (len(combined_format) > 0 and combined_format != set(['date-time'])):
        raise Exception('Only "date-time" JSON format supported')

    schema_keys = set(list(a.keys()) + list(b.keys()))
    if not schema_keys.issubset(ALLOWED_SIMPLE_SCHEMA_KEYS):
        raise Exception('Schema keys not allowed: {}'.format(schema_keys - ALLOWED_SIMPLE_SCHEMA_KEYS))

    ## if type has both integer and number, number should win
    if 'integer' in combined_type and 'number' in combined_type:
        combined_type.remove('integer')

    json_schema = {
        'type': sort_types(list(combined_type)),
    }

    if len(combined_format) > 1:
        json_schema['format'] = combined_format

    return json_schema

def merge_two_schemas(a, b):
    # we assume this is a schema generated by this module and make some assumptions
    combined_type = set(a['type'] + b['type'])
    if 'null' in combined_type:
        combined_type.remove('null')

    nested_count = 0
    if 'object' in combined_type:
        nested_count += 1
    if 'array' in combined_type:
        nested_count += 1

    ## if there is any combination of nested types (object, array) and simple types (integer, string, etc) use anyof
    if nested_count > 1 or (nested_count == 1 and len(combined_type - set(['object', 'array'])) > 0):
        anyof = []
        if 'object' in combined_type:
            anyof.append(merge_object_schemas(a, b))
        if 'array' in combined_type:
            anyof.append(merge_array_schemas(a, b))

        anyof.append(merge_simple_schemas(a, b))

        return {
            'anyOf': anyof
        }
    else:
        if 'object' in combined_type:
            return merge_object_schemas(a, b)
        if 'array' in combined_type:
            return merge_array_schemas(a, b)
        return merge_simple_schemas(a, b)


def merge_schemas(schemas):
    if len(schemas) == 1:
        return schemas[0]

    current_schema = None
    for schema in schemas:
        if current_schema is None:
            current_schema = schema
            continue
        current_schema = merge_two_schemas(current_schema, schema)

    return current_schema

def infer_type(value):
    if value == '':
        return None

    ## Some formats, like JSON, generate some typed values
    if isinstance(value, int):
        return 'integer'

    if isinstance(value, float):
        return 'number'

    if isinstance(value, bool):
        return 'boolean'

    try:
        int(value)
        return 'integer'
    except (ValueError, TypeError):
        pass

    try:
        float(value)
        return 'number'
    except (ValueError, TypeError):
        pass

    return 'string'

def infer_simple_type_schema(value):
    ## TODO: check datetime_fields and boolean_fields - support nested paths?

    ## Excel and other file formats have a datetime type
    if isinstance(value, datetime):
        return {
            'type': [
                'null',
                'string'
            ],
            'format': 'date-time'
        }

    ## TODO: if type is string, sniff for datetime formats

    json_schema = {
        'type': [
            'null'
        ]
    }

    json_type = infer_type(value)
    if json_type:
        json_schema['type'].append(json_type)

    return json_schema

def infer_array_schema(value):
    schemas = []
    for item in value:
        schemas.append(infer_schema(item))

    return {
        'type': [
            'null',
            'array'
        ],
        'items': merge_schemas(schemas)
    }

def infer_object_schema(obj):
    properties = {}
    for prop, value in obj.items():
        properties[prop] = infer_schema(value)

    return {
        'type': [
            'null',
            'object'
        ],
        'additionalProperties': False,
        'properties': properties
    }

def infer_schema(value):
    if isinstance(value, dict):
        return infer_object_schema(value)

    if isinstance(value, list):
        return infer_array_schema(value)

    return infer_simple_type_schema(value)

def schema_from_dict_sample(rows, stream_config):
    current_schema = None
    for row in rows:
        infered_schema = infer_schema(row)
        if current_schema is None:
            current_schema = infered_schema
            continue
        current_schema = merge_schemas([current_schema, infered_schema])

    return current_schema
