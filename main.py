from cgi import print_arguments

from ctypes import cast
from pandas import to_datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from datetime import date
import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, DateType
from processtwo import processtwo
from processone import processone    
import sys,json
from properties import *
from s3loader import *

###for s3 reader###
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.config("spark.driver.host", "localhost") \
    .getOrCreate()
print("\n\t***********Reading from s3***************")
s3 = s3()

df=spark.createDataFrame(s3.load_s3()) 
print(df.columns)
print(type(df))
print(df.show())

def copyintocurrent(df, target_tablename):
    df.write.mode('overwrite').jdbc(
        url='jdbc:postgresql://localhost:5432/Ebx_Acclerators_workspace',
        table=target_tablename,
        properties=connection)

def read_fromcurrent(target_tablename):
    return spark.read.jdbc(
        url='jdbc:postgresql://localhost:5432/Ebx_Acclerators_workspace',
        table=target_tablename,
        properties=connection)
copyintocurrent(df,'employee')
data = [{"completed": 'No'}]
df = spark.createDataFrame(data)
# df = spark.createDataFrame(
#     [
#         ('No'),  # create your data here, be consistent in the types.
#     ],
#     ["completed"]  # add your column names here
# )
copyintocurrent(df,"public.completion_check")

processone1 = processone(spark=spark)
processone1.copyFromSourceToCurrent(src_table_schema = 'public',primary_columns="empid",src_table_name='employee')