from asyncore import file_dispatcher
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging
from google.cloud import storage

def parquetize(file_):
    if not file_.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(file_)
    pq.write_table(table, file_.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, path, file_):
    """Uploads a file to the bucket."""
    
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(path)

    blob.upload_from_filename(file_)

    print(
        "File {} uploaded to {}.".format(
            file_, path
        )
    )
