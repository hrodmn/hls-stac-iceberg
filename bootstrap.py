import logging
import os
from typing import Literal

import obstore
import pyarrow.parquet as pq
from obstore.fsspec import FsspecStore
from obstore.store import S3Store
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import MonthTransform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CollectionIds = Literal["HLSS30_2.0", "HLSL30_2.0"]

YEARS = [2023, 2024, 2025]
SOURCE_PREFIX = "file-staging/nasa-map/hls-stac-geoparquet-archive/v2"
DEST_PREFIX = "hive-partitioned"
PARQUET_KEY_FORMAT = "{prefix}/{collection_id}/year={year}/month={month}/{collection_id}-{year}-{month}.parquet"

LOCAL_ICEBERG_ENDPOINT = os.getenv("ICEBERG_ENDPOINT", "http://rest:8181")
LOCAL_ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "default")
LOCAL_S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
LOCAL_S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")
LOCAL_S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
LOCAL_S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
LOCAL_S3_REGION = os.getenv("AWS_REGION", "us-east-1")
LOCAL_S3_ALLOW_HTTP = True

LOCAL_S3STORE_OPTIONS = {
    "bucket": LOCAL_S3_BUCKET,
    "endpoint": LOCAL_S3_ENDPOINT,
    "access_key_id": LOCAL_S3_ACCESS_KEY_ID,
    "secret_access_key": LOCAL_S3_SECRET_ACCESS_KEY,
    "region": LOCAL_S3_REGION,
    "allow_http": LOCAL_S3_ALLOW_HTTP,
}


def copy_remote_to_minio(
    dest_store: S3Store,
    collection_id: CollectionIds,
    years: list[int] = YEARS,
) -> list[str]:
    """Copy files from remote S3 bucket to MinIO."""
    source_store = S3Store(
        "nasa-maap-data-store",
        aws_skip_signature="true",
        region="us-west-2",
    )

    # Generate list of keys for 2024 and 2025
    source_keys = []
    dest_keys = []

    for year in years:
        for month in range(1, 13):
            source_key = PARQUET_KEY_FORMAT.format(
                prefix=SOURCE_PREFIX,
                collection_id=collection_id,
                year=year,
                month=month,
            )
            dest_key = PARQUET_KEY_FORMAT.format(
                prefix=DEST_PREFIX,
                collection_id=collection_id,
                year=year,
                month=month,
            )
            source_keys.append(source_key)
            dest_keys.append(dest_key)

    logger.info(f"Copying {len(source_keys)} files from remote S3 to MinIO...")

    copied = []
    for i, (source_key, dest_key) in enumerate(zip(source_keys, dest_keys), 1):
        try:
            _ = obstore.head(dest_store, dest_key)
            logger.info(f"{dest_key} already exists... skipping")
            copied.append(dest_key)
        except FileNotFoundError:
            logger.info(f"[{i}/{len(source_keys)}] Copying {source_key}...")
            data = source_store.get(source_key).bytes()
            dest_store.put(dest_key, data)
            copied.append(dest_key)

    return copied


def main(collection_id: CollectionIds = "HLSS30_2.0"):
    dest_store = S3Store(**LOCAL_S3STORE_OPTIONS)

    parquet_keys = copy_remote_to_minio(
        dest_store=dest_store,
        collection_id=collection_id,
        years=YEARS,
    )
    parquet_keys.sort()

    catalog = load_catalog(
        "rest",
        **{
            "uri": LOCAL_ICEBERG_ENDPOINT,
            "s3.endpoint": LOCAL_S3_ENDPOINT,
            "s3.access-key-id": LOCAL_S3_ACCESS_KEY_ID,
            "s3.secret-access-key": LOCAL_S3_SECRET_ACCESS_KEY,
            "s3.region": LOCAL_S3_REGION,
        },
    )

    try:
        catalog.create_namespace(LOCAL_ICEBERG_NAMESPACE)
        logger.info(f"Created namespace: {LOCAL_ICEBERG_NAMESPACE}")
    except Exception as e:
        logger.debug(f"Namespace may already exist: {e}")

    table_id = f"{LOCAL_ICEBERG_NAMESPACE}.{collection_id.replace('.', '_')}"

    # Read first file to get schema
    first_file = parquet_keys[0]
    logger.info(f"using {first_file} to generate the schema")

    fs = FsspecStore(
        "s3",
        region=LOCAL_S3_REGION,
        endpoint=LOCAL_S3_ENDPOINT,
        access_key_id=LOCAL_S3_ACCESS_KEY_ID,
        secret_access_key=LOCAL_S3_SECRET_ACCESS_KEY,
        client_options={"allow_http": LOCAL_S3_ALLOW_HTTP},
    )

    # Read table normally (keeps geometry as binary)
    df = pq.read_table(f"s3://{LOCAL_S3_BUCKET}/{first_file}", filesystem=fs)
    pyarrow_schema = df.schema
    logger.info(f"Schema:\n{pyarrow_schema}")

    # Extract GeoParquet metadata from schema metadata so we can attach it to the iceberg table
    # waiting on support for GeometryType in pyiceberg: https://github.com/apache/iceberg-python/issues/1820
    metadata = pq.read_metadata(f"s3://{LOCAL_S3_BUCKET}/{first_file}", filesystem=fs)
    geo_metadata = metadata.metadata.get(b"geo", None)

    if geo_metadata:
        logger.info(
            f"Found GeoParquet metadata: {geo_metadata.decode('utf-8')[:200]}..."
        )
    else:
        logger.warning("No GeoParquet metadata found in schema or geometry field")

    columns_to_drop = ["year", "month"]
    columns_to_keep = [col for col in df.column_names if col not in columns_to_drop]
    df = df.select(columns_to_keep)
    pyarrow_schema = df.schema

    logger.info(f"Dropped columns: {columns_to_drop}")

    # create a temporary table with auto-populated schema to scrape the iceberg schema
    temp_table_id = f"{table_id}_temp"
    try:
        catalog.drop_table(temp_table_id)
    except Exception:
        pass

    temp_table = catalog.create_table(temp_table_id, schema=pyarrow_schema)
    iceberg_schema = temp_table.schema()
    catalog.drop_table(temp_table_id)

    datetime_field = iceberg_schema.find_field("datetime")
    logger.info(f"Datetime field ID: {datetime_field.field_id}")

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=datetime_field.field_id,
            field_id=1000,
            transform=MonthTransform(),
            name="datetime_month",
        )
    )

    try:
        catalog.drop_table(table_id)
        logger.info(f"Dropped existing table: {table_id}")
    except Exception:
        pass

    logger.info("Creating table with month partitioning on datetime field...")

    # Prepare table properties with GeoParquet metadata
    table_properties = {}
    if geo_metadata:
        table_properties["geo"] = geo_metadata.decode("utf-8")
        logger.info("Adding GeoParquet metadata to table properties")

    table = catalog.create_table(
        table_id,
        schema=iceberg_schema,
        partition_spec=partition_spec,
        properties=table_properties,
    )
    table.append(df)
    n_rows = len(table.scan().to_arrow())

    logger.info(f"Created table and wrote {n_rows} records to {table._identifier}")

    for key in parquet_keys[1:]:
        logger.info(f"Processing {key}")
        df = pq.read_table(f"s3://warehouse/{key}", filesystem=fs)
        df = df.select(columns_to_keep)
        table.append(df)


if __name__ == "__main__":
    main()
