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
from random import randint
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
BUCKET = 'rawdata-zoom'
MIN_TIME_SLEEP = 0
MAX_TIME_SLEEP = 0
MAX_COUNTS = 5
TIMEOUT = 20
LOGS_UPFOLDER_BASE = 'air-meetings-logs'
ROOT_DIR = 'air-meetings-data'
# CUR_TIMESTAMP can be set as '2022-01-31' for exact date
# you may also use 'python zoom_load.py 2022-01-31' 
# when start script
CUR_TIMESTAMP = datetime.datetime.now()
# CUR_TIMESTAMP_CUT in format like 'YYYY-MM-DD'
# str(CUR_TIMESTAMP).split()[0] for current date
CUR_TIMESTAMP_CUT =  str(CUR_TIMESTAMP).split()[0]
CHUNK_SIZE = 16 * 1024

class ZoomLoader():
    def __init__(self, mount_path, bucket):
        self.mount_path = mount_path
        self.bucket = bucket
        self.logger = self.init_logger(f'{mount_path}/logs/zoom_load.log')
        self.logger.info('Zoom data loading started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_zoom_data = self.access_data(f'{mount_path}/access_zoom.json')
        self.jwt_token = self.access_zoom_data['jwtAccessToken']
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

    def load_logs_dates(self, from_time, to_time, logs_upfolder):
        output = None
        self.logger.info(f'uploading to bucket -> {self.bucket}, folder -> {logs_upfolder}')
        url = 'https://api.zoom.us/v2/accounts/me/recordings?from=' + from_time + '&to='+ to_time + '&page_size=300'
        try:
            data = self.get_content(url, timeout=TIMEOUT)
            data_enc = bytes(json.dumps(data, default=str).encode())
            filename = f'meetings_logs_{from_time}_{to_time}.json'
            filepath = f'{logs_upfolder}/{filename}'
            data_enc= json.loads(data_enc)
            self.s3.put_object(
                Body=data_enc, 
                Bucket=self.bucket, 
                Key=filepath
            )
            self.logger.info(f'{url} -> uploaded')
            output = data
        except Exception as e:
            self.logger.error(f'load logs by date for {url} - {e}')
        return output

    def load_meetings_data_chunks(self, meetings_data, root_dir, chunk_size):
        self.logger.info(f'load meetings data - total meetings {len(meetings_data["meetings"])}')
        count = 0
        for meeting in meetings_data['meetings']:
            try:
                for record_file in meeting['recording_files']:
                    file_name = '{}-{}.{}'.format(
                        record_file['recording_type'].replace('_', '-'),
                        record_file['id'],
                        record_file['file_extension']
                    )
                    file_path = root_dir + '/' + str(meeting['id']) + '/' + file_name
                    url = record_file['download_url'] + '?access_token=' + self.jwt_token
                    try:
                        response = urlopen(url)
                        with open(file_name, 'wb') as file:
                            while True:
                                chunk = response.read(chunk_size)
                                if not chunk:
                                    break
                                file.write(chunk)
                        self.s3.upload_file(
                            file_name,
                            self.bucket,
                            file_path
                        )
                        os.remove(file_name)
                    except Exception as e:
                        self.logger.error(f'load meetings data {url} - {e}')
                    self.logger.info('meeting {} uploading to bucket -> {}, folder -> {}'.format(
                        meeting["id"],
                        self.bucket,
                        file_path
                    ))
            except Exception as e:
                self.logger.error(f'load meetings data {meeting} - {e}')
            count += 1
        self.logger.info(f'load meetings data - finished {count}')

def load_all():
    if len(sys.argv) > 1:
        date_to_load = str(sys.argv[1])
        upfolder_name = str(sys.argv[1])
    else:
        date_to_load = CUR_TIMESTAMP_CUT
        upfolder_name = CUR_TIMESTAMP
    loader = ZoomLoader(mount_path=MOUNT_PATH, bucket=BUCKET)
    try:
        logs_upfolder = '-'.join([
            LOGS_UPFOLDER_BASE,
            str(upfolder_name).replace(' ', '-').replace(':', '-').replace('.', '-')
        ])
        meetings_data = loader.load_logs_dates(
            from_time=str(date_to_load),
            to_time=str(date_to_load),
            logs_upfolder=logs_upfolder
        )
        meetings_data = ast.literal_eval(meetings_data)
        loader.load_meetings_data_chunks(
            meetings_data, 
            root_dir=ROOT_DIR, 
            chunk_size=CHUNK_SIZE
        )
    except Exception as e:
        loader.logger.error(f'proc - {e}')

if __name__ == '__main__':
    load_all()
