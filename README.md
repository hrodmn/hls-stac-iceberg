# HLS STAC Iceberg

Local Apache Iceberg environment for [HLS STAC Geoparquet Archive](https://github.com/MAAP-Project/hls-stac-geoparquet-archive) data.

## Overview

Docker Compose stack that creates:
- **MinIO**: S3-compatible object storage (mocks AWS S3)
- **Iceberg REST**: Apache Iceberg catalog server
- **bootstrap**: [bootstrap.py](./bootstrap.py) downloads STAC geoparquet files from the HLS archive and loads them into partitioned Iceberg tables

The bootstrap container downloads monthly geoparquet files for the HLSS30_2.0 collection for years 2023-2025 from the public HLS STAC archive and writes them to MinIO. Tables are partitioned by month using the `datetime` field.

## Usage

Start the stack:
```bash
docker compose up
```

The bootstrap container runs automatically and populates the `default.HLSS30_2_0` table.

## Querying with DuckDB

To get the metadata file path, use PyIceberg:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "rest",
    uri="http://localhost:8181",
    **{
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }
)

table = catalog.load_table("default.HLSS30_2_0")
print(table.metadata_location)
```

Configure DuckDB to read from the local Iceberg tables:

```sql
INSTALL iceberg;
LOAD iceberg;

SET s3_endpoint='localhost:9000';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='admin';
SET s3_secret_access_key='password';

SELECT COUNT(*)
FROM iceberg_scan('{metadata_location}');
```

