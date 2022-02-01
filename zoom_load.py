import os
import re
import ast
import json
import boto3
import socket
import asyncio
import datetime
import threading
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

with open('access_s3.json') as file:
    access_s3_data = json.load(file)
with open('access_zoom.json') as file:
    access_zoom_data = json.load(file)

ZOOM_BUCKET = 'rawdata-zoom'
JWT_TOKEN = access_zoom_data['jwtAccessToken']
HEADER_ZOOM = 'Bearer ' + JWT_TOKEN
MIN_TIME_SLEEP = 0
MAX_TIME_SLEEP = 0
MAX_COUNTS = 5
TIMEOUT = 20
LOGS_UPFOLDER_BASE = 'air-meetings-logs'
ROOT_DIR = 'air-meetings-data'
CUR_TIMESTAMP = datetime.datetime.now()
CHUNK_SIZE = 16 * 1024

SESSION = boto3.session.Session()
S3 = SESSION.client(
    service_name='s3',
    aws_access_key_id=access_s3_data['aws_access_key_id'],
    aws_secret_access_key=access_s3_data['aws_secret_access_key'],
    endpoint_url='http://storage.yandexcloud.net'
)

def get_content(url_page, timeout, header, proxies=None, file=False):
    counts = 0
    content = None
    while counts < MAX_COUNTS:
        try:
            request = Request(url_page)
            request.add_header('authorization', header)
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
            print('URLError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        except HTTPError as e:
            counts += 1
            print('HTTPError | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        except socket.timeout as e:
            counts += 1
            print('socket timeout | ', url_page, ' | ', e, ' | counts: ', counts)
            sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
    return content

def load_logs_dates(from_time, to_time, logs_upfolder):
    output = None
    print(f'uploading to bucket->{ZOOM_BUCKET} folder->{logs_upfolder}')
    url = 'https://api.zoom.us/v2/accounts/me/recordings?from=' + from_time + '&to='+ to_time + '&page_size=300'
    try:
        data = get_content(url, timeout=TIMEOUT, header=HEADER_ZOOM)
        data_enc = bytes(json.dumps(data, default=str).encode())
        filename = f'meetings_logs_{from_time}_{to_time}.json'
        filepath = f'{logs_upfolder}/{filename}'
        data_enc= json.loads(data_enc)
        S3.put_object(
            Body=data_enc, 
            Bucket=ZOOM_BUCKET, 
            Key=filepath
        )
        print(url, '-> uploaded')
        output = data
    except Exception as e:
        print('ERROR', url, '|', e)
    return output

def load_meetings_data_chunks(meetings_data, root_dir, chunk_size):
    print('total meetings:', len(meetings_data['meetings']))
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
                url = record_file['download_url'] + '?access_token=' + JWT_TOKEN
                try:
                    response = urlopen(url)
                    with open(file_name, 'wb') as file:
                        while True:
                            chunk = response.read(chunk_size)
                            if not chunk:
                                break
                            file.write(chunk)
                    S3.upload_file(
                        file_name,
                        ZOOM_BUCKET,
                        file_path
                    )
                    os.remove(file_name)
                except Exception as e:
                    print('ERROR', url, '|', e)
                print(f'meeting {meeting["id"]} uploading to bucket->{ZOOM_BUCKET} folder->{file_path}')
        except Exception as e:
            print('ERROR at meeting:', meeting, '|', e)
        count += 1
    print('finished', count)

def load_all():
    logs_upfolder = '-'.join([
        LOGS_UPFOLDER_BASE,
        str(CUR_TIMESTAMP).replace(' ', '-').replace(':', '-').replace('.', '-')
    ])
    meetings_data = load_logs_dates(
        from_time=str(CUR_TIMESTAMP).split()[0], # format "YYYY-MM-DD"
        to_time=str(CUR_TIMESTAMP).split()[0], # format "YYYY-MM-DD"
        logs_upfolder=logs_upfolder
    )
    meetings_data = ast.literal_eval(meetings_data)
    load_meetings_data_chunks(meetings_data, root_dir=ROOT_DIR, chunk_size=CHUNK_SIZE)
    print('start timestamp:', CUR_TIMESTAMP)

if __name__ == "__main__":
    load_all()
