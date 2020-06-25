#WIP

# tap-files

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Extracts data from supported file-like storage systems and in supported formats.
- Supports date modified based incremental replication from some of the supported storage systems.

## Supported Storage Systems

This tap uses the [fsspec](https://github.com/intake/filesystem_spec) project to support many storage systems.


| System / Service | Incremental Replication Support |
| ---------------- | ------------------------------- |
| local file system | Y |
| FTP / SFTP | Y |
| S3 | Y |
| Google Cloud Storage (GCS) | |
| Azure Blob Storage / Azure Datalake Gen2 | |
| Dropbox | |
| HTTP / HTTPS | |
| HDFS / WebHDFS | |
| git | |
| Github | |

Storage locations in the `path`/`paths` parameter in the configuration use the scheme portion if the URL passed to determine the storage class to use. For example "s3://some-bucket/path/to/my/file.csv" where "s3" is the scheme. It defaults to the local filesystem.

The local file system would also support anything mounted to the local file system, such as NFS and SMB network file shares.

## Supported Formats

| Format | Extensions | Notes |
| ------ | ---------- | ----- |
| csv | .csv, .tsv | Defaults to "," delimiter. Defaults to tab delimiter if extension is .tsv |
| excel | .xlsx, .xls | |
| gis | .shp, .geojson, .ldgeojson | Supports converting spatial projection using the `to_crs` format option. Defaults to adding a `geom` field with stringified geojson. |
| json | .json, .ldjson | Also supported line-delimited JSON (ldjson) |

## Usage

TODO

## Configuration

TODO