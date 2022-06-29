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
BUCKET = 'rawdata-vk'
MIN_TIME_SLEEP = 1
MAX_TIME_SLEEP = 2
MAX_COUNTS = 5
TIMEOUT = 20
CUR_TIMESTAMP = datetime.datetime.now()
VER = '5.126'

class VKLoader():
    def __init__(self, mount_path, bucket):
        self.mount_path = mount_path
        self.bucket = bucket
        self.logger = self.init_logger(f'{mount_path}/logs/vk_load.log')
        self.logger.info('VKontakte data loading started')
        self.access_s3_data = self.access_data(f'{mount_path}/access_s3.json')
        self.access_vk_data = self.access_data(f'{mount_path}/access_vk_api.json')
        self.vk_token = self.access_vk_data['service_access_key']
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
        counts = 0
        flag = True
        while flag:
            data = self.get_content(
                url, timeout=TIMEOUT, 
                proxies=None, file=True
            )
            data = json.loads(data)
            if 'response' in data:
                flag = False
            else:
                counts += 1
                if counts > MAX_COUNTS: 
                    flag = False
                self.logger.info(f'get data from VK API -> repeat count {counts}')
                sleep(randint(counts * MIN_TIME_SLEEP, counts * MAX_TIME_SLEEP))
        return data

    def group_data_write_s3(self, group, method, fields, dir_name):
        output = None
        self.logger.info(f'uploading group {group} data to bucket -> {BUCKET}')
        try:
            url = ''.join([
                f'https://api.vk.com/method/{method}?',
                f'group_ids={group}',
                f'&fields={fields}',
                f'&access_token={self.vk_token}',
                f'&v={VER}'
            ])
            data = self.json_data(url)
            data_enc= json.dumps(data['response'])
            file_name = f'{group}.json'
            file_path = f'{dir_name}/{file_name}'
            self.s3.put_object(
                Body=data_enc, 
                Bucket=BUCKET, 
                Key=file_path
            )
            self.logger.info(f'{group} -> uploaded')
            output = data['response']    
        except Exception as e:
            self.logger.error(f'load group {group} - {e}')
        return output
    
    def wall_data_write_s3(self, owner_id, count, method, dir_name, verbose=True):
        output = None
        if verbose:
            self.logger.info(f'uploading owner_id {owner_id} wall posts to bucket -> {BUCKET}')
        try:
            offset = 0
            url = ''.join([
                f'https://api.vk.com/method/{method}?',
                f'owner_id=-{owner_id}',
                f'&offset={offset}',
                f'&count={count}',
                f'&access_token={self.vk_token}',
                f'&extended=1',
                f'&v={VER}'
            ])
            data = self.json_data(url)
            total_len = data['response']['count']
            while (offset + count) <= total_len:
                offset += count
                url = ''.join([
                    f'https://api.vk.com/method/{method}?',
                    f'owner_id=-{owner_id}',
                    f'&offset={offset}',
                    f'&count={count}',
                    f'&access_token={self.vk_token}',
                    f'&extended=1',
                    f'&v={VER}'
                ])
                data_tmp = self.json_data(url)
                data['response']['items'].extend(data_tmp['response']['items'])
                sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
            data_enc= json.dumps(data['response'])
            file_name = f'wall_owner_id_{owner_id}.json'
            file_path = f'{dir_name}/{file_name}'
            self.s3.put_object(
                Body=data_enc, 
                Bucket=BUCKET, 
                Key=file_path
            )
            if verbose:
                self.logger.info(f'owner_id {owner_id} wall posts {total_len} -> uploaded')
            output = data['response']    
        except Exception as e:
            if verbose:
                self.logger.error(f'load owner_id {owner_id} - {e}')
        return output
    
    def group_members_write_s3(self, group, count, method, dir_name):
        output = None
        self.logger.info(f'uploading group {group} members to bucket -> {BUCKET}')
        try:
            offset = 0
            url = ''.join([
                f'https://api.vk.com/method/{method}?',
                f'group_id={group}',
                f'&offset={offset}',
                f'&count={count}',
                f'&access_token={self.vk_token}',
                f'&v={VER}'
            ])
            data = self.json_data(url)
            total_len = data['response']['count']
            while (offset + count) <= total_len:
                offset += count
                url = ''.join([
                    f'https://api.vk.com/method/{method}?',
                    f'group_id={group}',
                    f'&offset={offset}',
                    f'&count={count}',
                    f'&access_token={self.vk_token}',
                    f'&v={VER}'
                ])
                data_tmp = self.json_data(url)
                data['response']['items'].extend(data_tmp['response']['items'])
                sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
            data_enc= json.dumps(data['response'])
            file_name = f'members_group_{group}.json'
            file_path = f'{dir_name}/{file_name}'
            self.s3.put_object(
                Body=data_enc, 
                Bucket=BUCKET, 
                Key=file_path
            )
            self.logger.info(f'group {group} members {total_len} -> uploaded')
            output = data['response']    
        except Exception as e:
            self.logger.error(f'load group {group} members - {e}')
        return output

    def group_members_full_write_s3(self, group, members, fields, count, method, dir_name):
        output = None
        self.logger.info(f'uploading members full data to bucket -> {BUCKET}')
        try:
            offset = 0
            total_len = len(members)
            members_str = ','.join([str(x) for x in members[offset : offset + count]])
            fields = ','.join(fields)
            url = ''.join([
                f'https://api.vk.com/method/{method}?',
                f'user_ids={members_str}',
                f'&fields={fields}',
                f'&access_token={self.vk_token}',
                f'&v={VER}'
            ])
            data = self.json_data(url)
            while (offset + count) <= total_len:
                offset += count
                members_str = ','.join([str(x) for x in members[offset : offset + count]])
                url = ''.join([
                    f'https://api.vk.com/method/{method}?',
                    f'user_ids={members_str}',
                    f'&fields={fields}',
                    f'&access_token={self.vk_token}',
                    f'&v={VER}'
                ])
                data_tmp = self.json_data(url)
                data['response'].extend(data_tmp['response'])
                sleep(randint(MIN_TIME_SLEEP, MAX_TIME_SLEEP))
            data_enc= json.dumps(data['response'])
            file_name = f'members_full_group_{group}.json'
            file_path = f'{dir_name}/{file_name}'
            self.s3.put_object(
                Body=data_enc, 
                Bucket=BUCKET, 
                Key=file_path
            )
            self.logger.info(f'group {group} members full data total {total_len} -> uploaded')
            output = data['response']    
        except Exception as e:
            self.logger.error(f'load group {group} members full data - {e}')
        return output

    def group_members_wall_write_s3(self, group, members, count, method, dir_name):
        output = None
        self.logger.info(f'uploading members wall data to bucket -> {BUCKET}')
        try:
            total_len = 0
            for owner_id in members:
                data_wall = self.wall_data_write_s3(
                    owner_id, 
                    count, 
                    method, 
                    dir_name,
                    verbose=False
                )
                if not data_wall: 
                    continue
                total_len += len(data_wall['items'])
            self.logger.info(f'group {group} members wall data total {total_len} -> uploaded')
            output = total_len
        except Exception as e:
            self.logger.error(f'load group {group} members wall data - {e}')
        return output

def load_all():
    # in a format 'gsom_ma', 'gsom_abiturient' or 'gsom.spbu'
    group = str(sys.argv[1])
    dir_name = f'{group.replace(".", "_")}-{str(CUR_TIMESTAMP).replace(" ", "-").replace(":", "-").replace(".", "-")}'
    loader = VKLoader(mount_path=MOUNT_PATH, bucket=BUCKET)
    try:
        # load data on group
        loader.logger.info(f'group parameter to load {group}')
        method = 'groups.getById'
        fields = ','.join([
            'id',
            'name',
            'screen_name',
            'is_closed',
            'deactivated',
            'is_admin',
            'admin_level',
            'is_member',
            'is_advertiser',
            'invited_by',
            'type',
            'photo_50',
            'photo_100',
            'photo_200',
            'activity',
            'addresses',
            'age_limits',
            'ban_info',
            'can_create_topic',
            'can_message',
            'can_post',
            'can_see_all_posts',
            'can_upload_doc',
            'can_upload_video',
            'city',
            'contacts',
            'counters',
            'country',
            'cover',
            'crop_photo',
            'description',
            'fixed_post',
            'has_photo',
            'is_favorite',
            'is_hidden_from_feed',
            'is_messages_blocked',
            'links',
            'main_album_id',
            'main_section',
            'market',
            'member_status',
            'members_count',
            'place',
            'public_date_label',
            'site',
            'start_date',
            'finish_date',
            'status',
            'trending',
            'verified',
            'wall',
            'wiki_page'
        ])
        data_group = loader.group_data_write_s3(
            group, 
            method, 
            fields, 
            dir_name
        )
        loader.logger.info(f'total groups {len(data_group)}')
        
        # load data on wall of the group
        count = 100
        owner_id = data_group[0]['id']
        method = 'wall.get'
        data_wall = loader.wall_data_write_s3(
            owner_id, 
            count, 
            method, 
            dir_name
        )
        loader.logger.info(f'total wall posts {len(data_wall["items"])}')
        
        # load data on members of the group
        method = 'groups.getMembers'
        data_members = loader.group_members_write_s3(
            group, 
            count, 
            method, 
            dir_name
        )
        loader.logger.info(f'total group members {len(data_members["items"])}')
        
        # load full data on members of the group
        method = 'users.get'
        members = data_members['items']
        fields = [
            # Базовые поля
            'id',
            'first_name',
            'last_name',
            'deactivated',
            'is_closed',
            'can_access_closed',
            'about',
            'activities',
            'bdate',
            'blacklisted',
            'blacklisted_by_me',
            'books',
            'can_post',
            'can_see_all_posts',
            'can_see_audio',
            'can_send_friend_request',
            'can_write_private_message',
            'career',
            'city',
            #'common_count',
            'connections',
            'contacts',
            'counters',
            'country',
            'crop_photo',
            'domain',
            'education',
            'exports',
            'first_name',
            'followers_count',
            'friend_status',
            'games',
            'has_mobile',
            'has_photo',
            'home_town',
            'interests',
            'is_favorite',
            'is_friend',
            'is_hidden_from_feed',
            'is_no_index',
            # Опциональные поля L-R
            'last_name',
            'last_seen',
            'lists',
            'maiden_name',
            'military',
            'movies',
            'music',
            'nickname',
            'occupation',
            'online',
            'personal',
            'photo_50',
            'photo_100',
            'photo_200_orig',
            'photo_200',
            'photo_400_orig',
            'photo_id',
            'photo_max',
            'photo_max_orig',
            'quotes',
            'relatives',
            'relation',
            # Опциональные поля S-W
            'schools',
            'screen_name',
            'sex',
            'site',
            'status',
            'timezone',
            'trending',
            'tv',
            'universities',
            'verified',
            'wall_default'
        ]
        data_members_full = loader.group_members_full_write_s3(
            group, 
            members, 
            fields, 
            count, 
            method, 
            dir_name
        )
        loader.logger.info(f'total group members full data {len(data_members_full)}')
        
        # load wall data on members of the group
        method = 'wall.get'
        data_members_wall = loader.group_members_wall_write_s3(
            group, 
            members, 
            count, 
            method, 
            f'{dir_name}/walls'
        )
        loader.logger.info(f'total group members wall data {data_members_wall}')
        
        loader.logger.info('VKontakte data load finished')
    except Exception as e:
        loader.logger.error(f'main load - {e}')

if __name__ == '__main__':
    load_all()
