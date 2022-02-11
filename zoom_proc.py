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
from pyspark.sql import udf
from pyspark.sql.types import *
from pyspark.sql.functions import struct
from pyspark.sql.functions import countDistinct
import multiprocessing

MOUNT_PATH = '/home/jovyan/zoomdataload'
BUCKET = 'rawdata-zoom'
STAGING_PATH = 'staging'
CUR_TIMESTAMP = datetime.datetime.now()
# CUR_TIMESTAMP_CUT in format like "YYYY-MM-DD"
# for mask to collect files by Spark
# str(CUR_TIMESTAMP).split()[0] for current date
# or empty string '' to collect all 
CUR_TIMESTAMP_CUT = str(CUR_TIMESTAMP).split()[0]
TEST = True

class ZoomProcessor():
    def __init__(self, mount_path, bucket, staging_path, mode):
        self.mount_path = mount_path
        self.bucket = bucket
        self.staging_path = staging_path
        self.mode = mode
        self.logger = self.init_logger(f'{mount_path}/logs/zoom_proc.log')
        self.logger.info('Zoom data processing started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_postgres = self.access_data(f'{mount_path}/access_postgres.json')
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
    
    def sdf_meetings_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            files = sdf.count()
            meetings = sdf.agg({'total_records': 'sum'}).rdd.flatMap(lambda x: x).collect()[0]
            if meetings > 0:
                sdf = sdf.select(F.explode(sdf.meetings))
                sdf = self.flat_df(sdf, prefix='meet_')
                sdf = sdf.withColumn(
                    'meet_start_time',
                    F.to_timestamp("meet_start_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")
                )
            else:
                sdf = None
            self.logger.info(f'meetings dataframe - total files {files}, total meetings: {meetings}')
        except Exception as e:
            self.logger.error(f'meetings dataframe - {e}')
        return sdf
    
    def sdf_records_preproc(self, sdf):
        try:
            sdf = sdf.select(
                sdf.meet_uuid,
                sdf.meet_id, 
                F.explode(sdf.meet_recording_files)
            )
            sdf = self.flat_df(sdf, prefix='rec_')
            sdf = sdf.withColumn(
                'rec_recording_start',
                F.to_timestamp("rec_recording_start", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            sdf = sdf.withColumn(
                'rec_recording_end',
                F.to_timestamp("rec_recording_end", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            records = sdf.count()
            self.logger.info(f'records dataframe done, {records} records')
        except Exception as e:
            self.logger.error(f'records dataframe - {e}')
        return sdf
    
    def sdf_participants_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            files = sdf.count()
            sdf = sdf.select(
                'uuid', 
                F.explode(
                    F.create_map(
                        F.lit('participants_data'), 
                        F.col('participants_data.participants')
                    )
                )
            )
            sdf = sdf.select(sdf.uuid, F.explode(sdf.value))
            sdf = self.flat_df(sdf)
            sdf = sdf.withColumn(
                'col_join_time',
                F.to_timestamp("col_join_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            sdf = sdf.withColumn(
                'col_leave_time',
                F.to_timestamp("col_leave_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")
            )
            sdf = sdf.withColumn(
                'col_internal_ip_addresses',
                F.concat_ws(",", F.col("col_internal_ip_addresses"))
            )
            pts = sdf.count()
            self.logger.info(f'participants dataframe - total files {files}, total participants: {pts}')
        except Exception as e:
            self.logger.error(f'meetings participants - {e}')
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
        df = self.spark.read.parquet(f's3a://{BUCKET}/{STAGING_PATH}/meetings')
        msg = f'meetings parquet: {df.count()}'
        print(msg)
        self.logger.debug(msg)
        query = '''
        SELECT * FROM meetings;
        '''
        result = self.send_query(query, res=True)
        msg = f'meetings database: {len(result)}'
        print(msg)
        self.logger.debug(msg)
        msg = f'sample: {result[0]}'
        print(msg)
        self.logger.debug(msg)

        df = self.spark.read.parquet(f's3a://{BUCKET}/{STAGING_PATH}/records')
        msg = f'records parquet: {df.count()}'
        print(msg)
        self.logger.debug(msg)
        query = '''
        SELECT * FROM records;
        '''
        result = self.send_query(query, res=True)
        msg = f'records database {len(result)}'
        print(msg)
        self.logger.debug(msg)
        msg = f'sample: {result[0]}'
        print(msg)
        self.logger.debug(msg)
        
        df = self.spark.read.parquet(f's3a://{BUCKET}/{STAGING_PATH}/participants')
        msg = f'records parquet: {df.count()}'
        print(msg)
        self.logger.debug(msg)
        query = '''
        SELECT * FROM participants;
        '''
        result = self.send_query(query, res=True)
        msg = f'records database {len(result)}'
        print(msg)
        self.logger.debug(msg)
        msg = f'sample: {result[0]}'
        print(msg)
        self.logger.debug(msg)
        print('------------ CHECK END --------------')

def proc():
    """
    Main process function. Takes sys.args to control load and process data iin a form:
      python <script_name> <head_title> <mode> <date>

        :head_title: 'hst' or 'air' for mask files for Spark load
        :mode: 'overwrite' or 'append'
        :date: in a format 'YYYY-MM-DD' or 'all' (all dates)
    
    """
    try:
        mask_files = '{}-meetings-logs-{}*/meetings_logs_{}*.json'.format(
            sys.argv[1],
            '' if sys.argv[3] == 'all' else  sys.argv[3],
            '' if sys.argv[3] == 'all' else  sys.argv[3]
        )
        mode = sys.argv[2]
    except:
        mask_files = '{}-meetings-logs-{}*/meetings_logs_{}*.json'.format(
            'air',
            CUR_TIMESTAMP_CUT,
            CUR_TIMESTAMP_CUT
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
        ############ MEETINGS #################
        #######################################

        sdf = processor.sdf_meetings_preproc(
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
            CREATE TABLE IF NOT EXISTS meetings (
                id bigserial primary key,
                meet_account_id varchar(22) NOT NULL,
                meet_duration bigint,
                meet_host_email varchar(128),
                meet_host_id varchar(22),
                meet_id bigint,
                meet_recording_count bigint,
                meet_share_url text,
                meet_start_time timestamp ,
                meet_timezone varchar(128),
                meet_topic text,
                meet_total_size bigint,
                meet_type bigint,
                meet_uuid varchar(24)
            );
            '''
            processor.send_query(query)
            sdf_save = sdf.select(
                sdf['meet_account_id'], 
                sdf['meet_duration'], 
                sdf['meet_host_email'], 
                sdf['meet_host_id'], 
                sdf['meet_id'], 
                sdf['meet_recording_count'], 
                sdf['meet_share_url'], 
                sdf['meet_start_time'], 
                sdf['meet_timezone'], 
                sdf['meet_topic'], 
                sdf['meet_total_size'],
                sdf['meet_type'], 
                sdf['meet_uuid']
            )
            processor.save_parquet(sdf=sdf_save, save_name='meetings')
            processor.save_spark_postgres(sdf=sdf_save, table_name='meetings')
            
            #######################################
            ############ RECORDS ##################
            #######################################
            
            sdf_rec = processor.sdf_records_preproc(sdf)
            query = '''
            CREATE TABLE IF NOT EXISTS records (
                id bigserial primary key,
                meet_uuid varchar(24) NOT NULL,
                meet_id bigint NOT NULL,
                rec_download_url text,
                rec_file_extension varchar(3),
                rec_file_size bigint,
                rec_file_type varchar(4),
                rec_id varchar(36),
                rec_meeting_id varchar(24),
                rec_play_url text,
                rec_recording_end timestamp,
                rec_recording_start timestamp,
                rec_recording_type varchar(128),
                rec_status varchar(128)
            );
            '''
            processor.send_query(query)
            processor.save_parquet(sdf=sdf_rec, save_name='records')
            processor.save_spark_postgres(sdf=sdf_rec, table_name='records')
            
            #######################################
            ############ PARTICIPANTS #############
            #######################################            
            
            mask_files = '*-meetings-data/*/participants_*.json'
            sdf_pts = processor.sdf_participants_preproc(
                file_path = f's3a://{BUCKET}/{mask_files}'
            )
            query = '''
            CREATE TABLE IF NOT EXISTS participants (
                id bigserial primary key,
                meeting_uuid varchar(24),
                camera text,
                connection_type varchar(8),
                customer_key text,
                data_center text,
                device text,
                domain text,
                email varchar(128),
                from_sip_uri text,
                full_data_center text,
                harddisk_id text,
                id varchar(22),
                internal_ip_addresses text,
                ip_address varchar(16),
                join_time timestamp,
                leave_reason text,
                leave_time timestamp,
                location text,
                mac_addr text,
                microphone text,
                network_type text,
                participant_user_id text,
                pc_name text,
                recording boolean,
                registrant_id string,
                role string,
                share_application boolean,
                share_desktop boolean,
                share_whiteboard boolean,
                sip_uri text,
                speaker text,
                status text,
                user_id varchar(9),
                user_name text,
                version text
            );
            GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbzoomreader;
            GRANT USAGE ON SCHEMA public TO dbzoomreader;
            '''
            processor.mode = 'overwrite'
            processor.send_query(query)
            processor.save_parquet(sdf=sdf_pts, save_name='participants')
            processor.save_spark_postgres(sdf=sdf_pts, table_name='participants')
            
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
