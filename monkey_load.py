#!/usr/bin/env python
# coding: utf-8

import os
import re
import sys
import ast
import json
import boto3
import socket
import logging
import datetime
from random import randint, getrandbits
from urllib.request import (
    Request,
    urlopen,
    URLError,
    HTTPError,
    ProxyHandler,
    build_opener,
    install_opener)
from urllib.parse import quote, unquote
from time import sleep, gmtime, strftime

MOUNT_PATH = '/home/jovyan/zoomdataload'
BUCKET = 'rawdata-monkey'
MIN_TIME_SLEEP = 0
MAX_TIME_SLEEP = 0
MAX_COUNTS = 5
TIMEOUT = 20
CUR_TIMESTAMP = datetime.datetime.now()

class MonkeyLoader():
    def __init__(self, mount_path, bucket):
        self.mount_path = mount_path
        self.bucket = bucket
        self.logger = self.init_logger(f'{mount_path}/logs/monkey_load.log')
        self.logger.info('SurveyMonkey data loading started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_monkey_data = self.access_data(f'{mount_path}/access_monkey.json')
        self.jwt_token = self.access_monkey_data['jwtAccessToken']
        self.header_zoom = 'Bearer ' + self.jwt_token
        try:
            session = boto3.session.Session()
            self.s3 = session.client(
                service_name='s3',
                aws_access_key_id=self.access_s3_data['aws_access_key_id'],
                aws_secret_access_key=self.access_s3_data['aws_secret_access_key'],
                endpoint_url='http://storage.yandexcloud.net'
            )
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

    def get_content(self, url_page, timeout, proxies=None, file=False):
        counts = 0
        content = None
        while counts < MAX_COUNTS:
            try:
                request = Request(url_page)
                request.add_header('authorization', self.header_zoom)
                if proxies:
                    proxy_support = ProxyHandler(proxies)
                    opener = build_opener(proxy_support)
                    install_opener(opener)
                    context = ssl._create_unverified_context()
                    response = urlopen(request, context=context, timeout=timeout)
                else:
                    response = urlopen(request, timeout=timeout)
                if file:
                    content = response.read()
                else:
                    try:
                        content = response.read().decode(response.headers.get_content_charset())
                    except:
                        content = None
                break
            except URLError as e:
                counts += 1
                self.logger.error(f'URLError | {url_page} | {e} | counts {counts}')
                sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
            except HTTPError as e:
                counts += 1
                self.logger.error(f'HTTPError | {url_page} | {e} | counts {counts}')
                sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
            except socket.timeout as e:
                counts += 1
                self.logger.error(f'socket timeout | {url_page} | {e} | counts {counts}')
                sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        return content

    def json_data(self, url):
        data = self.get_content(
            url, timeout=TIMEOUT, 
            proxies=None, file=True
        )
        return json.loads(data)

    def json_data_pages(self, url):
        data = self.json_data(url)
        while 'next' in data['links'].keys():
            data_ = self.json_data(data['links']['next'])
            data_['data'].extend(data['data']) 
            data = {**data, **data_}
        return data

    def surveys_write_s3(self, cur_date):
        output = None
        self.logger.info(f'uploading to bucket -> {BUCKET}')
        url = 'https://api.surveymonkey.com/v3/surveys?include=response_count,date_created,date_modified,language,question_count&per_page=300'
        try:
            data = self.json_data_pages(url)
            data_enc= json.dumps(data)
            file_name = f'surveys_{str(cur_date)}.json'
            file_path = f'{file_name}'
            self.s3.put_object(
                Body=data_enc, 
                Bucket=BUCKET, 
                Key=file_path
            )
            self.logger.info(f'{url} -> uploaded')
            output = data
        except Exception as e:
            self.logger.error(f'load survey {url} - {e}')
        return output

    def details_write_s3(self, data, upfolder, cut_date):
        all_surveys = []
        self.logger.info(f'uploading to bucket -> {BUCKET}, to folder -> {upfolder}')
        try:
            for s in data['data']:
                dtm = datetime.datetime.strptime(s['date_modified'], '%Y-%m-%dT%H:%M:%S')
                if dtm.date() >= cut_date:
                    url = s['href'] + '/details'
                    data = self.json_data(url)
                    data_enc= json.dumps(data)
                    file_name = f'survey_{s["id"]}.json'
                    file_path = f'{upfolder}/{file_name}'
                    self.s3.put_object(
                        Body=data_enc, 
                        Bucket=BUCKET, 
                        Key=file_path
                    )
                    self.logger.info(f'{url} -> uploaded')
                    all_surveys.append(data)
        except Exception as e:
            self.logger.error(f'load detailed surveys - {e}')
        return all_surveys    

    def responses_write_s3(self, all_surveys, upfolder):
        responses = []
        self.logger.info(f'uploading to bucket -> {BUCKET}, to folder -> {upfolder}')
        try:
            for s in all_surveys:
                url = s['href'] + "/responses/bulk?per_page=100"
                data = self.json_data_pages(url)
                data_enc= json.dumps(data)
                file_name = f'responses_{s["id"]}.json'
                file_path = f'{upfolder}/{file_name}'
                self.s3.put_object(
                    Body=data_enc, 
                    Bucket=BUCKET, 
                    Key=file_path
                )
                self.logger.info(f'{url} -> uploaded')
                responses.append(data)
        except Exception as e:
            self.logger.error(f'load responses - {e}')
        return responses

def load_all():
    if len(sys.argv) > 2:
        # in a format '2022-03-01'
        cur_date = datetime.datetime.strptime(str(sys.argv[1]), '%Y-%m-%d').date()
        lag_day = int(sys.argv[2])
    else:
        cur_date = CUR_TIMESTAMP.date()
        lag_day = int(sys.argv[1])
    loader = MonkeyLoader(mount_path=MOUNT_PATH, bucket=BUCKET)
    try:
        loader.logger.info(f'date parameter to load {cur_date}')
        data = loader.surveys_write_s3(cur_date)
        loader.logger.info(f'total surveys {len(data["data"])}')
        all_surveys = loader.details_write_s3(
            data, 
            upfolder='details',
            cut_date=cur_date - datetime.timedelta(days=lag_day)
        )
        loader.logger.info(f'fresh surveys to load {len(all_surveys)}')
        responses = loader.responses_write_s3(
            all_surveys, 
            upfolder='responses'
        )
        loader.logger.info(f'fresh responses to load {len(responses)}')
        loader.logger.info('SurveyMonkey data load finished')
    except Exception as e:
        loader.logger.error(f'main load - {e}')

if __name__ == '__main__':
    load_all()
