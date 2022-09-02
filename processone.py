from ctypes import cast
import this
from pandas import to_datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from datetime import date
import datetime
import time
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType, DateType
from sqlalchemy import table
from processtwo import processtwo
from properties import *


class processone:
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
            table=target_tablename.capitalize(),
            properties=self.connection)

  

    def copyFromSourceToCurrent(self,src_table_schema,src_table_name,primary_columns):
                print(src_table_name)
                df = self.read_fromstaging(target_tablename=src_table_name+'_cur')
                self.copy_intostaging(df, src_table_name+'_prv')
                non_primary_columns = ''
                #SQL for getting column names from DB table
                src_columns_sql ="(SELECT column_name FROM information_schema.columns WHERE table_schema ='"+src_table_schema+"' AND table_name = '"+src_table_name+"')as a"
            
                
                df_src_columns = self.read_fromstaging(src_columns_sql)
                data_collect = df_src_columns.collect()
                print(data_collect)
                
                all_columns = ''
                i= 0
                print('\n\tGenerating sql for md5 and non keys md5... ')
                pk_list = primary_columns.split(',')
                #generating nonkeys_md5 columns
                for row in data_collect:
                    all_columns = all_columns+' '+'"'+row['column_name']+'"'+' '+','
                    for j in pk_list: 
                        if row['column_name'] != j:
                                if i!=0:
                                    non_primary_columns = non_primary_columns+','+'"'+row['column_name']+'"'
                                elif i==0: 
                                    non_primary_columns = non_primary_columns+'"'+row['column_name']+'"'
                                i =i+1
                
                # if not(tfm_char_to_date is None or tfm_char_to_date ==''):
                #     tfm_var_list=tfm_char_to_date.split(",")
                #     for item in tfm_var_list:
                #         try:
                #              all_columns = all_columns.replace(item,"TO_TIMESTAMP("+item+", 'MM/DD/YYYY HH:mi:SS' )as "+item)
                #         except:
                #              all_columns = all_columns.replace(item,"TO_TIMESTAMP("+item+", 'YYYY/MM/DD HH:mi:SS' )as "+item)


                sqlmd5 = '(Select '+all_columns+'MD5(ROW("'+primary_columns+'")::TEXT) as keys_md5,MD5(ROW('+non_primary_columns+')::TEXT) as nonkeys_md5 FROM '+src_table_schema+'."'+src_table_name+'") as a'
                print(sqlmd5)
                print('\n\tGenetation of sql done, Reading from source db...')
                df_md5 =  self.read_fromstaging(sqlmd5)
                staging_table_schema =  "public"
                staging_table_name_cur =src_table_name+"_cur"
                print('\n\twriting from source to current...')
                
                self.copy_intostaging(df_md5,staging_table_schema+'.'+staging_table_name_cur)
                print('\n\tData loading done into table:'+staging_table_name_cur+'...')
                process2=  processtwo(self.spark)
                process2.run(src_table_name)
    
