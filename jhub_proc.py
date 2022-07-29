#!/usr/bin/env python
# coding: utf-8

import os
import sys
import json
import boto3
import logging
import psycopg2
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf, struct, countDistinct
import multiprocessing

MOUNT_PATH = '/home/jovyan/zoomdataload'
BUCKET = 'rawdata-logs-jhub'
STAGING_PATH = 'staging'
CUR_TIMESTAMP = datetime.datetime.now()
TEST = True

class ZoomProcessor():
    def __init__(self, mount_path, bucket, staging_path, mode):
        self.mount_path = mount_path
        self.bucket = bucket
        self.staging_path = staging_path
        self.mode = mode
        self.logger = self.init_logger(f'{mount_path}/logs/jhub_proc.log')
        self.logger.info('JupyterHub logs processing started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_postgres = self.access_data(f'{mount_path}/access_postgres_simba.json')
        try:
            session = boto3.session.Session()
            self.s3 = session.client(
                service_name='s3',
                aws_access_key_id=self.access_s3_data['aws_access_key_id'],
                aws_secret_access_key=self.access_s3_data['aws_secret_access_key'],
                endpoint_url='http://storage.yandexcloud.net'
            )
            conf = SparkConf()
            conf.set('spark.master', 'local[*]')
            conf.set('spark.executor.memory', '2G')
            sc = SparkContext(conf=conf)
            spark = SparkSession(sc)
            spark._jsc.hadoopConfiguration().set('fs.s3a.access.key', self.access_s3_data['aws_access_key_id'])
            spark._jsc.hadoopConfiguration().set('fs.s3a.secret.key', self.access_s3_data['aws_secret_access_key'])
            spark._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
            spark._jsc.hadoopConfiguration().set('fs.s3a.multipart.size', '104857600')
            spark._jsc.hadoopConfiguration().set('fs.s3a.block.size', '33554432')
            spark._jsc.hadoopConfiguration().set('fs.s3a.threads.max', '256')
            spark._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://storage.yandexcloud.net')
            self.spark = spark
            self.logger.info('S3 session and Spark - started')
        except Exception as e:
            self.logger.error(f'S3 session and Spark - {e}')

    def init_logger(self, log_file, name=None):
        """
        Inits logger.

        :log_file: path to save log file
        :name: set name for logger, default __name__

        """
        logger = logging.getLogger(name) if name else logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(log_file)
        logger.addHandler(fh)
        formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        fh.setFormatter(formatter)
        return logger
    
    def access_data(self, file_path):
        access_data = {}
        try:
            with open(file_path) as file:
                access_data = json.load(file)
            self.logger.info(f'credentials from file {file_path} - loaded')
        except Exception as e:
            self.logger.error(f'credentials from file {file_path} - {e}')
        return access_data
        
    def flat_df(self, df, prefix=None):
        flat_cols = [c[0] for c in df.dtypes if c[1][:6] != 'struct']
        nested_cols = [c[0] for c in df.dtypes if c[1][:6] == 'struct']
        flat_df = df.select(
            flat_cols + 
            [F.col(ncol + '.' + col).alias(prefix + col if prefix else ncol + '_' + col ) 
             for ncol in nested_cols 
             for col in df.select(ncol + '.*').columns]
        )
        return flat_df
    
    def send_query(self, query, res=False):
        result = None
        try:
            with psycopg2.connect(
                host=self.access_postgres['host'],
                port=self.access_postgres['port'],
                dbname=self.access_postgres['dbname'],
                user=self.access_postgres['user'],
                password=self.access_postgres['password'],
                target_session_attrs='read-write',
                sslmode='verify-full',
                sslrootcert=f'{self.mount_path}/{self.access_postgres["sslrootcert"]}'
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        if res:
                            result = cur.fetchall()
            self.logger.info('save meetings query {} done'.format(query.split()[0]))
        except Exception as e:
            self.logger.error('save meetings query {} - {}'.format(
                query.split()[0], str(e).replace('\n', '')
            ))
        return result
    
    def sdf_logs_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            files = sdf.count()
            sdf = self.flat_df(sdf, prefix='kuber_')
            sdf = sdf.drop('kuber_annotations', 'kuber_labels')
            
            def sqbrackets(sin):
                try:
                    s = sin.split('[', 1)[1].split(']')[0]
                    msg = sin[len(s) + 2 :].strip()
                    s = s.split()
                    head = s[0]
                    ts = ' '.join(s[1:3])
                    svc = s[3]
                    typ = s[4].split(':')[0]
                    code = s[4].split(':')[1]
                except:
                    head, ts, svc, typ, code = '', '', '', '', ''
                    msg = sin
                return head, ts, svc, typ, code, msg
            
            udf_sqbrackets = udf(sqbrackets, ArrayType(StringType()))
            sdf = sdf.withColumn('log_msg', udf_sqbrackets('log'))
            sdf = sdf.select(
                F.col('time').alias('time_stamp'),
                'kuber_container_name',
                'kuber_host',
                'kuber_pod_name',
                F.col('log_msg')[0].alias('log_head'),
                F.col('log_msg')[1].alias('log_timestamp'),
                F.col('log_msg')[2].alias('log_service'),
                F.col('log_msg')[3].alias('log_type'),
                F.col('log_msg')[4].alias('log_code'),
                F.col('log_msg')[5].alias('log_msg')
            )
            sdf = sdf.withColumn(
                'time_stamp',
                F.to_timestamp("time_stamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'")
            )
            sdf = sdf.withColumn(
                'log_timestamp',
                F.to_timestamp("log_timestamp", "yyyy-MM-dd HH:mm:ss.SSS")
            )
            self.logger.info(f'hub logs dataframe - total files {files}')
        except Exception as e:
            self.logger.error(f'hub logs dataframe - {e}')
        return sdf
        
    def save_spark_postgres(self, sdf, table_name):
        """
        Saves Spark dataframe to PostgreSQL database.

        :sdf: Spark dataframe
        :access_data: credentials to access database
        :table_name: table in database to write data
        :mode: specifies the behavior of the save operation when data already exists.
            append: Append contents of this DataFrame to existing data.
            overwrite: Overwrite existing data.
            ignore: Silently ignore this operation if data already exists.
            error or errorifexists (default case): Throw an exception if data already exists.

        """
        try:
            url = 'jdbc:postgresql://{}:{}/{}'.format(
                self.access_postgres["host"],
                self.access_postgres["port"],
                self.access_postgres["dbname"]
            )
            sdf.write \
                .mode(self.mode) \
                .format('jdbc') \
                .option('url', url) \
                .option('dbtable', table_name) \
                .option('user', self.access_postgres['user']) \
                .option('password', self.access_postgres['password']) \
                .option('driver', 'org.postgresql.Driver') \
                .option('ssl', True) \
                .option('sslmode', 'require') \
                .save()
            self.logger.info('write Spark dataframe to PostgreSQL - done')
            return True
        except Exception as e:
            self.logger.error(f'write Spark dataframe to PostgreSQL - {e}')
            return False

    def save_parquet(self, sdf, save_name):
        try:
            sdf.write \
                .mode(self.mode) \
                .parquet(f's3a://{self.bucket}/{self.staging_path}/{save_name}')
            self.logger.info(f'save to parquet - {self.staging_path}/{save_name}')
            return True
        except Exception as e:
            self.logger.error(f'save to parquet - {e}')
            return False
    
    def s3_all_files(self, mask=''):
        all_files = [
            key['Key'] for key 
            in self.s3.list_objects(Bucket=self.bucket)['Contents'] 
            if mask in key['Key']
        ]
        return all_files, len(all_files)
    
    def check_loaded(self):
        print('----------- CHECK START -------------')
        df = self.spark.read.parquet(f's3a://{BUCKET}/{STAGING_PATH}/jhublogs')
        msg = f'logs parquet: {df.count()}'
        print(msg)
        self.logger.debug(msg)
        query = '''
        SELECT * FROM jhublogs;
        '''
        result = self.send_query(query, res=True)
        msg = f'logs database: {len(result)}'
        print(msg)
        self.logger.debug(msg)
        msg = f'sample: {result[0]}'
        print(msg)
        self.logger.debug(msg)
        print('------------ CHECK END --------------')

def proc():
    """
    Main process function. Takes sys.args to control load and process data in a form:
      python <script_name> <all>

        :date(s): 'all' for total logs load
    
    """
    try:
        if sys.argv[1] == 'all':
            mask_files = '{}/{}/{}/{}/{}/*/*'.format(
                'fluent-bit-logs/kube.var.log.containers.hub*',
                '*', # year
                '*',   # month
                '*',   # day
                '*'    # hour
            )
            mode = 'overwrite'
    except:
        lts = CUR_TIMESTAMP - datetime.timedelta(hours=1)
        mask_files = '{}/{}/{}/{}/{}/*/*'.format(
            'fluent-bit-logs/kube.var.log.containers.hub*',
            str(lts.year),                                              # year
            '0' + str(lts.month) if lts.month < 10 else str(lts.month), # month
            '0' + str(lts.day) if lts.day < 10 else str(lts.day),       # day
            '0' + str(lts.hour) if lts.hour < 10 else str(lts.hour)     # hour
        )
        mode = 'append'
        
    processor = ZoomProcessor(
        mount_path=MOUNT_PATH, 
        bucket=BUCKET, 
        staging_path=STAGING_PATH, 
        mode=mode
    )
    try:
        all_files, num_files = processor.s3_all_files(mask='')
        msg = f'{len(all_files)} files in a bucket {BUCKET}'
        processor.logger.info(msg)

        #######################################
        ########## JUPYTERHUB LOGS ############
        #######################################
        sdf = processor.sdf_logs_preproc(
            file_path = f's3a://{BUCKET}/{mask_files}'
        )
        if sdf:
            query = '''
            SELECT tablename AS table FROM pg_tables WHERE tablename !~ '^(pg_|sql_)';
            '''
            # drop table if necessary
            #query = '''
            #DROP TABLE dummy_table;
            #'''
            processor.send_query(query)
            query = '''
            CREATE TABLE IF NOT EXISTS jhublogs (
                id bigserial primary key,
                time_stamp timestamp,
                kuber_container_name varchar(64),
                kuber_host varchar(128),
                kuber_pod_name varchar(128),
                log_head varchar(8),
                log_timestamp timestamp,
                log_service varchar(64),
                log_type varchar(32),
                log_code varchar(32),
                log_msg text
            );
            '''
            processor.send_query(query)
            sdf_save = sdf.select(
                sdf['time_stamp'],
                sdf['kuber_container_name'],
                sdf['kuber_host'],
                sdf['kuber_pod_name'],
                sdf['log_head'],
                sdf['log_timestamp'],
                sdf['log_service'],
                sdf['log_type'],
                sdf['log_code'],
                sdf['log_msg']
            )
            processor.save_parquet(sdf=sdf_save, save_name='jhublogs')
            processor.save_spark_postgres(sdf=sdf_save, table_name='jhublogs')
            
            if TEST:
                processor.check_loaded()
        else:
            processor.logger.info('no data to process')
    except Exception as e:
        processor.logger.error(f'proc - {e}')
    
    processor.spark.stop()
    msg = 'finished'
    processor.logger.info(msg)

if __name__ == '__main__':
    proc()
