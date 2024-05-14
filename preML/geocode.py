os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = XXX
#import tensorflow as tf
import os
import pandas as pd
from datetime import datetime
import pandas_gbq
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler
import pandas_gbq


query = """
SELECT
 *
FROM
  `az-voter-file.Data_AVP_001.masterAddresses`
"""
df = pandas_gbq.read_gbq(query, project_id="az-voter-file")
df.head()
df.to_csv('to_geocode.csv') ## For later load, not to sync.