from ctypes import cast
from pandas import to_datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import date
import datetime
import time
from pyspark.sql.types import IntegerType,BooleanType,DateType
from properties import *

class processtwo:
    
    spark = None

    def __init__(self, spark):
        self.spark = spark

    connection = {'url': 'jdbc:postgresql://localhost:5432/Ebx_Acclerators_workspace',
                  'user': 'postgres',
                  'password': 'admin',
                  'schema': 'public',
                  'driver': 'org.postgresql.Driver',
                  'fetchSize': '2000'}

    def copy_intostaging(self, df, target_tablename):
        df.write.mode('overwrite').jdbc(
            url=self.connection['url'],
            table=target_tablename,
            properties=self.connection)

    def read_fromstaging(self, target_tablename):
        return self.spark.read.jdbc(
            url=self.connection['url'],
            table=target_tablename,
            properties=self.connection)
    def run(self,src_table_name):
        spark = self.spark
        staging_table_schema =  "public"
        staging_table_name_cur =src_table_name+"_cur"
        staging_table_name_prv = src_table_name+"_prv"
        staging_table_name_premdm = src_table_name+"_premdm"

        staging_columns_sql ="(SELECT column_name FROM information_schema.columns WHERE table_schema ='"+staging_table_schema+"' AND table_name = '"+staging_table_name_cur+"')as a"
        print('\n\tGetting sql for comparrasion ...')
        print(staging_columns_sql)
        df = self.read_fromstaging(staging_columns_sql)
        print(df.show())
        data_collect = df.collect()
        postmd5sql =''
        for row in data_collect:
            if row['column_name'] == 'nonkeys_md5':
                ''
            elif row['column_name'] == 'keys_md5':
                ''
            else:
                postmd5sql = postmd5sql+'case  when s.'+row['column_name']+' is null then t.'+row['column_name']+' else s.'+row['column_name']+' end as '+row['column_name']+',\n'


        s = """case
                    when s.keys_md5 is null and t.keys_md5 is not null  then 'D' 
                    when t.keys_md5 is null then 'A' 
                    when s.keys_md5=t.keys_md5 and s.nonkeys_md5 <> t.nonkeys_md5 then 'U'
                    else 'O'   end as change_code
                FROM  s full outer join  t on s.keys_md5=t.keys_md5;
                """

        postmd5sql = f"select "+postmd5sql+s

        print(postmd5sql)

        s = self.read_fromstaging(staging_table_schema+'.'+staging_table_name_cur)
        t = self.read_fromstaging(staging_table_schema+'.'+staging_table_name_prv)
        print(t.columns)
        print(s.columns)
        s.createOrReplaceTempView('s')
        t.createOrReplaceTempView('t')
        print("\n\tStarting Comparison...")
        into_premdm = spark.sql(postmd5sql)
        print(into_premdm.show())
        into_premdm = into_premdm.dropDuplicates(['empid'])
        self.copy_intostaging(df=into_premdm,target_tablename=staging_table_schema+'.'+staging_table_name_premdm)
        data = [{"completed": 'yes'}]
        df = spark.createDataFrame(data)
        self.copy_intostaging(df,"public.completion_check")
        # into_premdmfnl = into_premdm.where(into_premdm.change_code != 'O')  
        # today = date.today()
        # Version = today.strftime("%Y%m%d") 
        # into_premdmfnl = into_premdmfnl.withColumn('version_number',F.lit(Version))

        # if tfm_int_to_date is not None:
        #     tfm_var_list=tfm_int_to_date.split(",")
        #     for item in tfm_var_list:
        #         for item1 in into_premdmfnl.columns:
        #             if item == item1:
        #                 print('\n\t\tinside transform')
                        
        #                 into_premdmfnl = into_premdmfnl.withColumn(item, F.to_date(F.col(item).cast("string"),'yyyy-MM-dd'))      

        # if tfm_char_to_date is not None:
        #     tfm_var_list=tfm_char_to_date.split(",")
        #     for item in tfm_var_list:
        #         for item1 in into_premdmfnl.columns:
        #             if item == item1:
        #                 print('\n\t\tinside transform')
                        
        #                 into_premdmfnl = into_premdmfnl.withColumn(item, F.to_timestamp(F.col(item),'yyyy-MM-dd HH:MM:SS'))      


        # print('\n\tinserting into table :'+staging_table_schema+'.'+staging_table_name_premdm)
        # self.copy_intostaging(into_premdmfnl,staging_table_schema+'.'+staging_table_name_premdm)
  