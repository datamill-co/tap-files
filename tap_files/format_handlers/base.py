from datetime import datetime, timezone

import singer
from singer import metrics, Transformer

from tap_files.discover_utils import schema_from_dict_sample
from tap_files.discover_utils import SDC_SOURCE_PATH_COLUMN

LOGGER = singer.get_logger()

class BaseFormatHandler:
    format_name = None
    extensions = []
    default_extension = None
    file_mode = 'rt'

    def sync(self, stream_config, schema, mdata, path, ext, file):
        stream_name = stream_config['stream_name']
        reader = self._get_rows_reader(stream_config, ext, file)

        time_extracted = datetime.now(timezone.utc)
        with metrics.record_counter(stream_name) as counter:
            with Transformer() as transformer:
                for record in reader:
                    self._add_path_to_record(record, path)

                    if schema:
                        try:
                            record = transformer.transform(record,
                                                           schema,
                                                           mdata)
                        except:
                            LOGGER.info('Tranformation error: {}'.format(record))
                            raise

                    singer.write_record(stream_name, record, time_extracted=time_extracted)
                    counter.increment()

    def discover(self, stream_config, path, ext, file):
        reader = self._get_rows_reader(stream_config, ext, file)

        discover_sample_size = stream_config.get('format_options', {}).get('discover_sample_size', 10000)
        rows = []
        try:
            for i in range(discover_sample_size):
                rows.append(self._add_path_to_record(next(reader), path))
        except StopIteration:
            pass

        return schema_from_dict_sample(rows, stream_config)

    def _add_path_to_record(self, record, path):
        record[SDC_SOURCE_PATH_COLUMN] = path
        return record

    def _get_rows_reader(self, stream_config, ext, file):
        raise NotImplementedError()
