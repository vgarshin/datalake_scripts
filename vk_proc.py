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
BUCKET = 'rawdata-vk'
STAGING_PATH = 'staging'
CUR_TIMESTAMP = datetime.datetime.now()

class VKProcessor():
    def __init__(self, mount_path, bucket, staging_path, mode):
        self.mount_path = mount_path
        self.bucket = bucket
        self.staging_path = staging_path
        self.mode = mode
        self.logger = self.init_logger(f'{mount_path}/vk_proc.log')
        self.logger.info('VKontakte data processing started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_postgres = self.access_data(f'{mount_path}/access_postgres_vk.json')
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
    
    def sdf_group_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            groups = sdf.count()
            sdf = self.flat_df(sdf, prefix='')
            sdf = sdf.withColumnRenamed('id', 'group_id')
            sdf = sdf.withColumn(
                'load_datetime',
                F.lit(CUR_TIMESTAMP)
            )
            self.logger.info(f'groups dataframe - total groups {groups}')
        except Exception as e:
            self.logger.error(f'groups dataframe - {e}')
        return sdf
    
    def sdf_group_contacts_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.group_id, sdf.load_datetime, F.explode(sdf.contacts))
            sdf = self.flat_df(sdf, prefix='contacts_')
            contacts = sdf.count()
            self.logger.info(f'group contacts dataframe - total contacts {contacts}')
        except Exception as e:
            self.logger.error(f'group contacts dataframe - {e}')
        return sdf
    
    def sdf_group_links_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.group_id, sdf.load_datetime, F.explode(sdf.links))
            sdf = self.flat_df(sdf, prefix='links_')
            links = sdf.count()
            self.logger.info(f'group links dataframe - total contacts {links}')
        except Exception as e:
            self.logger.error(f'group links dataframe - {e}')
        return sdf
    
    def sdf_member_preproc(self, group_id, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            sdf = self.flat_df(sdf, prefix='')
            sdf = sdf.withColumn('group_id', F.lit(group_id))
            sdf = sdf.withColumnRenamed('id', 'member_id')
            sdf = sdf.withColumn('load_datetime', F.lit(CUR_TIMESTAMP))
            sdf = sdf.withColumn(
                'last_seen_time',
                F.to_timestamp("last_seen_time")
            )
            members = sdf.count()
            self.logger.info(f'members dataframe - total members {members}')
        except Exception as e:
            self.logger.error(f'members dataframe - {e}')
        return sdf
    
    def sdf_member_career_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.member_id, sdf.load_datetime, F.explode(sdf.career))
            sdf = self.flat_df(sdf, prefix='career_')
            careers = sdf.count()
            self.logger.info(f'member career dataframe - total careers {careers}')
        except Exception as e:
            self.logger.error(f'member career dataframe - {e}')
        return sdf
    
    def sdf_member_schools_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.member_id, sdf.load_datetime, F.explode(sdf.schools))
            sdf = self.flat_df(sdf, prefix='schools_')
            schools = sdf.count()
            self.logger.info(f'member schools dataframe - total schools {schools}')
        except Exception as e:
            self.logger.error(f'member schools dataframe - {e}')
        return sdf
    
    def sdf_member_universities_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.member_id, sdf.load_datetime, F.explode(sdf.universities))
            sdf = self.flat_df(sdf, prefix='universities_')
            universities = sdf.count()
            self.logger.info(f'member universities dataframe - total universities {universities}')
        except Exception as e:
            self.logger.error(f'member universities dataframe - {e}')
        return sdf
    
    def sdf_wall_preproc(self, file_path):
        sdf = None
        try:
            sdf = self.spark.read.json(
                file_path
            )
            sdf = self.flat_df(sdf, prefix='')
            sdf = sdf.withColumn(
                'load_datetime',
                F.lit(CUR_TIMESTAMP)
            )
            posts = sdf.count()
            self.logger.info(f'wall dataframe - total wall {posts}')
        except Exception as e:
            self.logger.error(f'wall dataframe - {e}')
        return sdf
    
    def sdf_wall_items_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.load_datetime, F.explode(sdf.items))
            sdf = self.flat_df(sdf, prefix='items_')
            sdf = self.flat_df(sdf)
            sdf = sdf.withColumn(
                'items_date',
                F.to_timestamp('items_date')
            )
            sdf = sdf.withColumn(
                'items_edited',
                F.to_timestamp('items_edited')
            )
            items = sdf.count()
            self.logger.info(f'wall items dataframe - total items {items}')
        except Exception as e:
            self.logger.error(f'wall items dataframe - {e}')
        return sdf
    
    def sdf_wall_history_preproc(self, sdf):
        try:
            sdf = sdf.select(sdf.load_datetime, F.explode(sdf.items))
            sdf = self.flat_df(sdf)
            sdf = sdf.select(sdf.load_datetime, F.explode(sdf.col_copy_history))
            sdf = self.flat_df(sdf, prefix='history_')
            sdf = self.flat_df(sdf)
            sdf = sdf.withColumn(
                'history_date',
                F.to_timestamp('history_date')
            )
            histories = sdf.count()
            self.logger.info(f'wall histories dataframe - total histories {histories}')
        except Exception as e:
            self.logger.error(f'wall histories dataframe - {e}')
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
        :date: date to write in from '2022-04-25'
        
      if no <mode> <date> are given then script runs for current date
    
    """
    try:
        if sys.argv[1] == 'overwrite':
            proc_date = sys.argv[2]
            mode = 'overwrite'
    except:
        proc_date = str(CUR_TIMESTAMP).split()[0]
        mode = 'append'
    
    processor = VKProcessor(
        mount_path=MOUNT_PATH, 
        bucket=BUCKET, 
        staging_path=STAGING_PATH, 
        mode=mode
    )
    try:
        msg = f'processing date {proc_date}'
        processor.logger.info(msg)

        #######################################
        ############### GROUPS ################
        #######################################
        
        sdf = processor.sdf_group_preproc(
            file_path = f's3a://{BUCKET}/*{proc_date}*/gsom_ma.json'
        )
        sdf_save = sdf.select(
            sdf['load_datetime'], 
            sdf['group_id'], 
            sdf['type'],
            sdf['name'], 
            sdf['screen_name'], 
            sdf['activity'],
            sdf['description'], 
            sdf['is_closed'], 
            sdf['members_count'], 
            sdf['status'], 
            sdf['verified'],
            sdf['site'], 
            sdf['wiki_page'],
            sdf['city_id'],
            sdf['city_title'],
            sdf['country_id'],
            sdf['country_title']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups (
            id bigserial primary key,
            load_datetime timestamp,
            group_id bigint NOT NULL,
            type varchar(128),
            name varchar(128),
            screen_name varchar(128),
            activity text,
            description text,
            is_closed int,
            members_count bigint,
            status text,
            verified int,
            site varchar(128),
            wiki_page text,
            city_id bigint,
            city_title varchar(128),
            country_id bigint,
            country_title varchar(128)
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_save, save_name='hst_groups')
        processor.save_spark_postgres(sdf=sdf_save, table_name='hst_groups')
        
        sdf_c = processor.sdf_group_contacts_preproc(sdf)
        sdf_c_save = sdf_c.select(
            sdf_c['load_datetime'], 
            sdf_c['group_id'], 
            sdf_c['contacts_desc'],
            sdf_c['contacts_email'], 
            sdf_c['contacts_phone']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_contacts;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_contacts (
            id bigserial primary key,
            load_datetime timestamp,
            group_id bigint NOT NULL,
            contacts_desc text,
            contacts_email varchar(128),
            contacts_phone varchar(32)
        )
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_c_save, save_name='hst_groups_contacts')
        processor.save_spark_postgres(sdf=sdf_c_save, table_name='hst_groups_contacts')
        
        sdf_l = processor.sdf_group_links_preproc(sdf)
        sdf_l_save = sdf_l.select(
            sdf_l['load_datetime'], 
            sdf_l['group_id'], 
            sdf_l['links_id'],
            sdf_l['links_name'], 
            sdf_l['links_desc'],
            sdf_l['links_url']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_links;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_links (
            id bigserial primary key,
            load_datetime timestamp,
            group_id bigint NOT NULL,
            links_id bigint NOT NULL,
            links_name varchar(128),
            links_desc text,
            links_url varchar(128)
        )
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_l_save, save_name='hst_groups_links')
        processor.save_spark_postgres(sdf=sdf_l_save, table_name='hst_groups_links')
        
        #######################################
        ############## MEMBERS ################
        #######################################
        
        group_id = sdf_c.select('group_id').rdd.flatMap(lambda x: x).collect()[0]
        sdf_m = processor.sdf_member_preproc(
            group_id,
            file_path = f's3a://{BUCKET}/*{proc_date}*/members_full_group_gsom_ma.json'
        )
        sdf_m_save = sdf_m.select(
            sdf_m['load_datetime'], 
            sdf_m['group_id'], 
            sdf_m['member_id'],
            sdf_m['first_name'],
            sdf_m['last_name'],
            sdf_m['maiden_name'],
            sdf_m['screen_name'],
            sdf_m['nickname'],
            sdf_m['sex'],
            sdf_m['city_id'],
            sdf_m['city_title'],
            sdf_m['home_town'],
            sdf_m['country_id'],
            sdf_m['country_title'],
            sdf_m['about'], 
            sdf_m['activities'],
            sdf_m['books'],
            sdf_m['can_post'],
            sdf_m['deactivated'],
            sdf_m['domain'],
            sdf_m['followers_count'],
            sdf_m['friend_status'],
            sdf_m['games'],
            sdf_m['interests'],
            sdf_m['is_closed'],
            sdf_m['is_friend'],
            sdf_m['personal'],
            sdf_m['site'],
            sdf_m['skype'],
            sdf_m['livejournal'],
            sdf_m['twitter'],
            sdf_m['has_mobile'],
            sdf_m['mobile_phone'],
            sdf_m['home_phone'],
            sdf_m['status'],
            sdf_m['relation'],
            sdf_m['relation_partner_id'],
            sdf_m['relation_partner_first_name'],
            sdf_m['relation_partner_last_name'],
            sdf_m['education_form'],
            sdf_m['education_status'],
            sdf_m['faculty'],
            sdf_m['faculty_name'],
            sdf_m['graduation'],
            sdf_m['university'],
            sdf_m['university_name'],
            sdf_m['occupation_id'],
            sdf_m['occupation_name'],
            sdf_m['occupation_type'],
            sdf_m['movies'],
            sdf_m['music'],
            sdf_m['trending'],
            sdf_m['tv'],
            sdf_m['verified'],
            sdf_m['wall_default'],
            sdf_m['last_seen_platform'],
            sdf_m['last_seen_time']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_members;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_members (
            id bigserial primary key,
            load_datetime timestamp,
            group_id bigint NOT NULL, 
            member_id bigint NOT NULL,
            first_name varchar(128),
            last_name varchar(128),
            maiden_name varchar(128),
            screen_name varchar(128),
            nickname varchar(128),
            sex int,
            city_id varchar(16),
            city_title varchar(128),
            home_town varchar(128),
            country_id varchar(16),
            country_title varchar(128),
            about text, 
            activities text,
            books text,
            can_post int,
            deactivated int,
            domain varchar(128),
            followers_count varchar(16),
            friend_status int,
            games text,
            interests text,
            is_closed boolean,
            is_friend int,
            personal text,
            site varchar(128),
            skype varchar(128),
            livejournal varchar(128),
            twitter varchar(128),
            has_mobile int,
            mobile_phone varchar(128),
            home_phone varchar(128),
            status varchar(128),
            relation varchar(16),
            relation_partner_id varchar(16),
            relation_partner_first_name varchar(128),
            relation_partner_last_name varchar(128),
            education_form varchar(128),
            education_status varchar(128),
            faculty varchar(128),
            faculty_name varchar(128),
            graduation varchar(16),
            university varchar(16),
            university_name varchar(128),
            occupation_id varchar(16),
            occupation_name varchar(128),
            occupation_type varchar(128),
            movies varchar(16),
            music varchar(16),
            trending varchar(16),
            tv varchar(16),
            verified varchar(16),
            wall_default varchar(16),
            last_seen_platform varchar(16),
            last_seen_time timestamp
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_m_save, save_name='hst_groups_members')
        processor.save_spark_postgres(sdf=sdf_m_save, table_name='hst_groups_members')
        
        sdf_m_c = processor.sdf_member_career_preproc(sdf_m)
        sdf_m_c_save = sdf_m_c.select(
            sdf_m_c['load_datetime'], 
            sdf_m_c['member_id'],
            sdf_m_c['career_city_id'],
            sdf_m_c['career_country_id'],
            sdf_m_c['career_company'],
            sdf_m_c['career_group_id'],
            sdf_m_c['career_position'],
            sdf_m_c['career_from'],
            sdf_m_c['career_until']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_members_careers;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_members_careers (
            id bigserial primary key,
            load_datetime timestamp,
            member_id bigint NOT NULL,
            career_city_id varchar(16),
            career_country_id varchar(16),
            career_company varchar(128),
            career_group_id varchar(16),
            career_position varchar(128),
            career_from varchar(16),
            career_until varchar(16)
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_m_c_save, save_name='hst_groups_members_careers')
        processor.save_spark_postgres(sdf=sdf_m_c_save, table_name='hst_groups_members_careers')
        
        sdf_m_s = processor.sdf_member_schools_preproc(sdf_m)
        sdf_m_s_save = sdf_m_s.select(
            sdf_m_s['load_datetime'], 
            sdf_m_s['member_id'],
            sdf_m_s['schools_city'],
            sdf_m_s['schools_class'],
            sdf_m_s['schools_country'],
            sdf_m_s['schools_id'],
            sdf_m_s['schools_name'],
            sdf_m_s['schools_speciality'],
            sdf_m_s['schools_type'],
            sdf_m_s['schools_type_str'],
            sdf_m_s['schools_year_from'],
            sdf_m_s['schools_year_graduated'],
            sdf_m_s['schools_year_to']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_members_schools;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_members_schools (
            id bigserial primary key,
            load_datetime timestamp,
            member_id bigint NOT NULL,
            schools_city varchar(16),
            schools_class varchar(16),
            schools_country varchar(16),
            schools_id varchar(16),
            schools_name varchar(256),
            schools_speciality varchar(128),
            schools_type varchar(16),
            schools_type_str varchar(128),
            schools_year_from varchar(16),
            schools_year_graduated varchar(16),
            schools_year_to varchar(16)
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_m_s_save, save_name='hst_groups_members_schools')
        processor.save_spark_postgres(sdf=sdf_m_s_save, table_name='hst_groups_members_schools')
        
        sdf_m_u = processor.sdf_member_universities_preproc(sdf_m)
        sdf_m_u_save = sdf_m_u.select(
            sdf_m_u['load_datetime'], 
            sdf_m_u['member_id'],
            sdf_m_u['universities_chair'],
            sdf_m_u['universities_chair_name'],
            sdf_m_u['universities_city'],
            sdf_m_u['universities_country'],
            sdf_m_u['universities_education_form'],
            sdf_m_u['universities_education_status'],
            sdf_m_u['universities_faculty'],
            sdf_m_u['universities_faculty_name'],
            sdf_m_u['universities_graduation'],
            sdf_m_u['universities_id'],
            sdf_m_u['universities_name']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_groups_members_universities;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_groups_members_universities (
            id bigserial primary key,
            load_datetime timestamp,
            member_id bigint NOT NULL,
            universities_chair varchar(16),
            universities_chair_name varchar(128),
            universities_city varchar(16),
            universities_country varchar(16),
            universities_education_form varchar(128),
            universities_education_status varchar(128),
            universities_faculty varchar(16),
            universities_faculty_name varchar(128),
            universities_graduation varchar(16),
            universities_id varchar(16),
            universities_name varchar(128)
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_m_u_save, save_name='hst_groups_members_universities')
        processor.save_spark_postgres(sdf=sdf_m_u_save, table_name='hst_groups_members_universities')
        
        #######################################
        ############### WALLS #################
        #######################################
        
        sdf_w = processor.sdf_wall_preproc(
            file_path = f's3a://{BUCKET}/*{proc_date}*/wall_owner_id_*.json'
        )
        sdf_w_i = processor.sdf_wall_items_preproc(sdf_w)
        sdf_w_i_save = sdf_w_i.select(
            sdf_w_i['load_datetime'], 
            sdf_w_i['items_owner_id'],
            sdf_w_i['items_from_id'],
            sdf_w_i['items_id'],
            sdf_w_i['items_date'],
            sdf_w_i['items_edited'],
            sdf_w_i['items_post_type'],
            sdf_w_i['items_text'],
            sdf_w_i['items_comments_count'],
            sdf_w_i['items_donut_is_donut'],
            sdf_w_i['items_likes_count'],
            sdf_w_i['items_likes_user_likes'],
            sdf_w_i['items_post_source_type'],
            sdf_w_i['items_reposts_count'],
            sdf_w_i['items_reposts_user_reposted'],
            sdf_w_i['items_views_count']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_wall_items;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_wall_items (
            id bigserial primary key,
            load_datetime timestamp,
            items_owner_id varchar(16),
            items_from_id varchar(16),
            items_id varchar(16),
            items_date timestamp,
            items_edited timestamp,
            items_post_type varchar(16),
            items_text text,
            items_comments_count bigint,
            items_donut_is_donut boolean,
            items_likes_count bigint,
            items_likes_user_likes bigint,
            items_post_source_type varchar(128),
            items_reposts_count bigint,
            items_reposts_user_reposted bigint,
            items_views_count bigint
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_w_i_save, save_name='hst_wall_items')
        processor.save_spark_postgres(sdf=sdf_w_i_save, table_name='hst_wall_items')
        
        sdf_w_h = processor.sdf_wall_history_preproc(sdf_w)
        sdf_w_h_save = sdf_w_h.select(
            sdf_w_h['load_datetime'], 
            sdf_w_h['history_id'],
            sdf_w_h['history_from_id'],
            sdf_w_h['history_owner_id'],
            sdf_w_h['history_date'],
            sdf_w_h['history_post_type'],
            sdf_w_h['history_text'],
            sdf_w_h['history_post_source_platform'],
            sdf_w_h['history_post_source_type']
        )
        if mode == 'overwrite':
            query = '''
            DROP TABLE hst_wall_history;
            '''
            processor.send_query(query)
        query = '''
        CREATE TABLE IF NOT EXISTS hst_wall_history (
            id bigserial primary key,
            load_datetime timestamp,
            history_id varchar(16),
            history_from_id varchar(16),
            history_owner_id varchar(16),
            history_date timestamp,
            history_post_type varchar(16),
            history_text text,
            history_post_source_platform varchar(16),
            history_post_source_type varchar(16)
        );
        '''
        processor.send_query(query)
        processor.save_parquet(sdf=sdf_w_h_save, save_name='hst_wall_history')
        processor.save_spark_postgres(sdf=sdf_w_h_save, table_name='hst_wall_history')
    except Exception as e:
        processor.logger.error(f'proc - {e}')
    
    processor.spark.stop()
    msg = 'finished'
    processor.logger.info(msg)

if __name__ == '__main__':
    proc()
