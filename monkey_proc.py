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
BUCKET = 'rawdata-monkey'
STAGING_PATH = 'staging'
CUR_TIMESTAMP = datetime.datetime.now()

class MonkeyProcessor():
    def __init__(self, mount_path, bucket, staging_path, mode):
        self.mount_path = mount_path
        self.bucket = bucket
        self.staging_path = staging_path
        self.mode = mode
        self.logger = self.init_logger(f'{mount_path}/logs/monkey_proc.log')
        self.logger.info('SurveyMonkey data processing started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_postgres = self.access_data(f'{mount_path}/access_postgres_monkey.json')
        try:
            session = boto3.session.Session()
            self.s3 = session.client(
                service_name='s3',
                aws_access_key_id=self.access_s3_data['aws_access_key_id'],
                aws_secret_access_key=self.access_s3_data['aws_secret_access_key'],
                endpoint_url='http://storage.yandexcloud.net'
            )
            conf = SparkConf()
            conf.set('spark.master', 'local[2]')
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
    
    def sdf_surveys_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            surveys = sdf.count()
            sdf = sdf.withColumn(
                'date_created',
                F.to_timestamp("date_created", "yyyy-MM-dd'T'HH:mm:ss") # 2021-12-26T10:40:00
            )
            sdf = sdf.withColumn(
                'date_modified',
                F.to_timestamp("date_modified", "yyyy-MM-dd'T'HH:mm:ss")
            )
            sdf = sdf.withColumnRenamed('id', 'survey_id')
            sdf = sdf.withColumn(
                'load_datetime',
                F.lit(CUR_TIMESTAMP)
            )
            self.logger.info(f'surveys dataframe - total surveys {surveys}')
        except Exception as e:
            self.logger.error(f'surveys dataframe - {e}')
        return sdf
    
    def sdf_responses_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            responses = sdf.count()
            sdf = sdf.withColumn(
                'load_datetime',
                F.lit(CUR_TIMESTAMP)
            )
            self.logger.info(f'responses dataframe - total responses {responses}')
        except Exception as e:
            self.logger.error(f'responses dataframe - {e}')
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
            sdf.write\
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

def proc():
    """
    Main process function. Takes sys.args to control load and process data in a form:
      python <script_name> <mode> <date>

        :mode: 'overwrite' to overwrite all data
        
      if no <mode> is given then script runs in 'append' (default) mode
    
    """
    try:
        if sys.argv[1] == 'overwrite':
            mode = 'overwrite'
    except:
        mode = 'append'
    
    processor = MonkeyProcessor(
        mount_path=MOUNT_PATH, 
        bucket=BUCKET, 
        staging_path=STAGING_PATH, 
        mode=mode
    )
    try:
        msg = f'processing data, load datetime {CUR_TIMESTAMP}, mode {mode}'
        processor.logger.info(msg)

        #######################################
        ############## SURVEYS ################
        #######################################
        
        sdf = processor.sdf_surveys_preproc(
            file_path = f's3a://{BUCKET}/details/survey_*.json'
        )
        sdf_save = sdf.select(
            sdf['load_datetime'], 
            sdf['survey_id'], 
            sdf['date_created'],
            sdf['date_modified'],
            sdf['folder_id'],
            sdf['language'],
            sdf['page_count'], 
            sdf['question_count'], 
            sdf['response_count'],
            sdf['title']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_surveys;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_surveys (
            id bigserial primary key,
            load_datetime timestamp,
            survey_id bigint NOT NULL, 
            date_created timestamp,
            date_modified timestamp,
            folder_id bigint,
            language varchar(8),
            page_count bigint, 
            question_count bigint, 
            response_count bigint,
            title text
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_save, save_name='hst_surveys')
        processor.save_spark_postgres(sdf=sdf_save, table_name='hst_surveys')
        
        #######################################
        ############## DETAILS ################
        #######################################
        
        ########### Pages-Questions ###########
        
        sdf_p = sdf.select(sdf.survey_id, sdf.load_datetime, F.explode(sdf.pages))
        sdf_p = processor.flat_df(sdf_p, prefix='pages_')
        sdf_q = sdf_p.select(
            sdf_p.load_datetime,
            sdf_p.survey_id, 
            sdf_p.pages_id,
            sdf_p.pages_position,
            sdf_p.pages_question_count,
            sdf_p.pages_title,
            F.explode(sdf_p.pages_questions)
        )
        sdf_q = processor.flat_df(sdf_q, prefix='qs_')
        sdf_q = sdf_q.select(
            sdf_q.load_datetime,
            sdf_q.survey_id, 
            sdf_q.pages_id,
            sdf_q.qs_id,
            sdf_q.qs_position,
            F.explode(sdf_q.qs_headings),
            sdf_q.qs_answers
        )
        sdf_q = processor.flat_df(sdf_q, prefix='headings_')
        sdf_q_save = sdf_q.select(
            sdf_q['load_datetime'], 
            sdf_q['survey_id'], 
            sdf_q['pages_id'],
            sdf_q['qs_id'],
            sdf_q['qs_position'],
            sdf_q['headings_heading']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_surveys_questions;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_surveys_questions (
            id bigserial primary key,
            load_datetime timestamp,
            survey_id bigint NOT NULL, 
            pages_id bigint,
            qs_id bigint,
            qs_position bigint,
            headings_heading text
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_q_save, save_name='hst_surveys_questions')
        processor.save_spark_postgres(sdf=sdf_q_save, table_name='hst_surveys_questions')
        
        ############### Choices ###############
        
        sdf_c = sdf_q.select(
            sdf_q.load_datetime,
            sdf_q.survey_id, 
            sdf_q.pages_id,
            sdf_q.qs_id,
            sdf_q.qs_position,
            F.explode(sdf_q.headings_choices)
        )
        sdf_c = processor.flat_df(sdf_c, prefix='choices_')
        sdf_c = processor.flat_df(sdf_c, prefix='choices_quiz_options_')
        sdf_c_save = sdf_c.select(
            sdf_c['load_datetime'], 
            sdf_c['survey_id'], 
            sdf_c['pages_id'],
            sdf_c['qs_id'],
            sdf_c['qs_position'],
            sdf_c['choices_id'],
            sdf_c['choices_is_na'],
            sdf_c['choices_position'],
            sdf_c['choices_quiz_options_score'],
            sdf_c['choices_text'],
            sdf_c['choices_visible'],
            sdf_c['choices_weight']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_surveys_choices;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_surveys_choices (
            id bigserial primary key,
            load_datetime timestamp,
            survey_id bigint NOT NULL, 
            pages_id bigint,
            qs_id bigint,
            qs_position bigint,
            choices_id bigint,
            choices_is_na boolean,
            choices_position bigint,
            choices_quiz_options_score varchar(128),
            choices_text text,
            choices_visible boolean,
            choices_weight bigint
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_c_save, save_name='hst_surveys_choices')
        processor.save_spark_postgres(sdf=sdf_c_save, table_name='hst_surveys_choices')

        ############## Responses ##############
        
        sdf = processor.sdf_responses_preproc(
            file_path = f's3a://{BUCKET}/responses/responses_*.json'
        )
        sdf_r = sdf.select(
            sdf.load_datetime,
            F.explode(sdf.data)
        )
        sdf_r = processor.flat_df(sdf_r, prefix='response_')
        sdf_r_save = sdf_r.select(
            sdf_r['load_datetime'], 
            sdf_r['response_id'],
            sdf_r['response_survey_id'],
            sdf_r['response_date_created'],
            sdf_r['response_date_modified'],
            sdf_r['response_email_address'],
            sdf_r['response_ip_address'],
            sdf_r['response_first_name'],
            sdf_r['response_last_name'],
            sdf_r['response_recipient_id'],
            sdf_r['response_response_status'],
            sdf_r['response_total_time']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_surveys_responses;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_surveys_responses (
            id bigserial primary key,
            load_datetime timestamp,
            response_id bigint NOT NULL,
            response_survey_id bigint,
            response_date_created timestamp,
            response_date_modified timestamp,
            response_email_address varchar(128),
            response_ip_address varchar(128),
            response_first_name varchar(128),
            response_last_name varchar(128),
            response_recipient_id bigint,
            response_response_status varchar(128),
            response_total_time bigint
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_r_save, save_name='hst_surveys_responses')
        processor.save_spark_postgres(sdf=sdf_r_save, table_name='hst_surveys_responses')
        
        ######### Detailed responses ##########

        sdf_p = sdf_r.select(
            sdf_r.load_datetime,
            sdf_r.response_id, 
            sdf_r.response_survey_id,
            sdf_r.response_recipient_id,
            F.explode(sdf_r.response_pages)
        )
        sdf_p = processor.flat_df(sdf_p, prefix='pages_')
        sdf_p = sdf_p.select(
            sdf_p.load_datetime,
            sdf_p.response_id, 
            sdf_p.response_survey_id,
            sdf_p.response_recipient_id,
            sdf_p.pages_id,
            F.explode(sdf_p.pages_questions)
        )
        sdf_p = processor.flat_df(sdf_p, prefix='questions_')
        sdf_q = sdf_p.select(
            sdf_p.load_datetime,
            sdf_p.response_id, 
            sdf_p.response_survey_id,
            sdf_p.response_recipient_id,
            sdf_p.pages_id,
            sdf_p.questions_id,
            F.explode(sdf_p.questions_answers)
        )
        sdf_q = processor.flat_df(sdf_q, prefix='questions_answers_')
        sdf_q = processor.flat_df(sdf_q, prefix='choices_questions_answers_')
        sdf_q_save = sdf_q.select(
            sdf_q['load_datetime'], 
            sdf_q['response_id'],
            sdf_q['response_survey_id'],
            sdf_q['response_recipient_id'],
            sdf_q['pages_id'],
            sdf_q['questions_id'],
            sdf_q['questions_answers_choice_id'],
            sdf_q['questions_answers_row_id'],
            sdf_q['questions_answers_text'],
            sdf_q['choices_questions_answers_weight']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_surveys_answers;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_surveys_answers (
            id bigserial primary key,
            load_datetime timestamp,
            response_id bigint NOT NULL,
            response_survey_id bigint,
            response_recipient_id bigint,
            pages_id bigint,
            questions_id bigint,
            questions_answers_choice_id bigint,
            questions_answers_row_id bigint,
            questions_answers_text text,
            choices_questions_answers_weight bigint
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_q_save, save_name='hst_surveys_answers')
        processor.save_spark_postgres(sdf=sdf_q_save, table_name='hst_surveys_answers')
    except Exception as e:
        processor.logger.error(f'proc - {e}')
    
    processor.spark.stop()
    msg = 'finished'
    processor.logger.info(msg)

if __name__ == '__main__':
    proc()
