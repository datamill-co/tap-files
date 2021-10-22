from datetime import datetime, timezone

import singer
from singer import metrics, metadata, Transformer

from tap_files.discover_utils import schema_from_dict_sample
from tap_files.discover_utils import SDC_SOURCE_PATH_COLUMN, SDC_SOURCE_MODIFIED_DATE_COLUMN

LOGGER = singer.get_logger()

class BaseFormatHandler:
    format_name = None
    extensions = []
    default_extension = None
    file_mode = 'rt'

    def sync(self, stream_config, catalog, path, ext, file, modified_date):
        if stream_config.get('use_streams'):
            base_stream_name = stream_config['stream_name']
            streams = self._get_streams(stream_config, file)

            for stream_name, reader_gen in streams:
                self._sync_stream(stream_name, stream_config, catalog, ext, path, file, modified_date, reader_gen)
        else:
            stream_name = stream_config['stream_name']
            self._sync_stream(stream_name, stream_config, catalog, ext, path, file, modified_date, None)
        # reader = self._get_rows_reader(stream_config, ext, file, reader_gen=reader_gen)

        # time_extracted = datetime.now(timezone.utc)
        # with metrics.record_counter(stream_name) as counter:
        #     with Transformer() as transformer:
        #         for record in reader:
        #             self._add_metadata_to_record(record, path, modified_date)

        #             if schema:
        #                 try:
        #                     record = transformer.transform(record,
        #                                                    schema,
        #                                                    mdata)
        #                 except:
        #                     LOGGER.info('Tranformation error: {}'.format(record))
        #                     raise

        #             singer.write_record(stream_name, record, time_extracted=time_extracted)
        #             counter.increment()

    def _sync_stream(self, stream_name, stream_config, catalog, ext, path, file, modified_date, reader_gen):
        schema = None
        mdata = None
        if catalog:
            for stream in catalog.streams:
                if stream.tap_stream_id == stream_name:
                    schema = stream.schema.to_dict()
                    mdata = metadata.to_map(stream.metadata)
                    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)

        reader = self._get_rows_reader(stream_name, stream_config, ext, file, reader_gen=reader_gen)

        time_extracted = datetime.now(timezone.utc)
        with metrics.record_counter(stream_name) as counter:
            with Transformer() as transformer:
                for record in reader:
                    self._add_metadata_to_record(record, path, modified_date)

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

    def discover(self, stream_config, path, ext, file, modified_date):
        schemas = {}
        if stream_config.get('use_streams'):
            base_stream_name = stream_config['stream_name']
            streams = self._get_streams(stream_config, file)

            for stream_name, reader_gen in streams:
                schemas[stream_name] = self._discover_stream(stream_name, stream_config, path, ext, file, modified_date, reader_gen)
        else:
            stream_name = stream_config['stream_name']
            schemas[stream_name] = self._discover_stream(stream_name, stream_config, path, ext, file, modified_date, None)
        return schemas

    def _discover_stream(self, stream_name, stream_config, path, ext, file, modified_date, reader_gen):
        reader = self._get_rows_reader(stream_name, stream_config, ext, file, reader_gen=reader_gen)

        discover_method = stream_config.get('discover_method', 'sample')
        discover_sample_size = stream_config.get('discover_sample_size', 10000)
        rows = []

        def get_rows():
            record = next(reader)
            rows.append(self._add_metadata_to_record(record, path, modified_date))

        try:
            if discover_method == 'sample':
                for i in range(discover_sample_size):
                    get_rows()
            elif discover_method == 'full':
                for record in reader:
                    get_rows()
            else:
                raise Exception('Discover method "{}" not supported'.format(discover_method))
        except StopIteration:
            pass

        return schema_from_dict_sample(rows, stream_config)

    def _add_metadata_to_record(self, record, path, modified_date):
        record[SDC_SOURCE_PATH_COLUMN] = path

        if modified_date:
            record[SDC_SOURCE_MODIFIED_DATE_COLUMN] = modified_date

        return record

    def _get_rows_reader(self, stream_config, ext, file):
        raise NotImplementedError()
