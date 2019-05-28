
# access data
from s3fs import S3FileSystem

s3 = S3FileSystem(anon=True)
s3.ls('dask-data/nyc-taxi/2015/')

# import data
import pandas as pd

with s3.open('dask-data/nyc-taxi/2015/yellow_tripdata_2015-01.csv') as f:
    df = pd.read_csv(f, nrows=5, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

df.head()

# DASK
from dask.distributed import Client, progress
client = Client('18.223.255.68:8786')
#client = Client('18.222.238.200:8786')
client
