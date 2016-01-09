#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# lager - an easy tool to access cloud storage service,
#         and also, abstract and simplify corresponding API
# Copyright (C) 2015-present Himawari Tachibana <fieliapm@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
################################################################################


from . import storage
from . import sign_url


def new_storage_from_config(storage_class_name, server_name, config):
    if storage_class_name == 'GoogleCloudStorage':
        kwargs = {
            'bucket_name': config['GCS_BUCKET_NAME'],
            'project': config['GC_PROJECT_NAME'],
        }
        if 'GC_JSON_CREDENTIALS_PATH' in config:
            kwargs['json_credentials_path'] = config['GC_JSON_CREDENTIALS_PATH']
        if 'GC_JSON_CREDENTIALS_STRING' in config:
            kwargs['json_credentials_string'] = config['GC_JSON_CREDENTIALS_STRING']
    elif storage_class_name == 'GoogleCloudStorage_Boto':
        kwargs = {
            'bucket_name': config['GCS_BUCKET_NAME'],
            'gs_access_key_id': config['GCS_ACCESS_KEY_ID'],
            'gs_secret_access_key': config['GCS_SECRET_ACCESS_KEY'],
            'is_interoperability_mode': True,
        }
    elif storage_class_name == 'AmazonS3Storage':
        kwargs = {
            'bucket_name': config['AWS_S3_BUCKET_NAME'],
            'aws_access_key_id': config['AWS_ACCESS_KEY_ID'],
            'aws_secret_access_key': config['AWS_SECRET_ACCESS_KEY'],
        }
    elif storage_class_name == 'AmazonCloudFrontS3Storage':
        kwargs = {
            'distribution_id': config['AWS_CF_DISTRIBUTION_ID'],
            'key_pair_id': config['AWS_CF_KEY_PAIR_ID'],
            'private_key_string': config['AWS_CF_PRIVATE_KEY_STRING'],
            'aws_access_key_id': config['AWS_ACCESS_KEY_ID'],
            'aws_secret_access_key': config['AWS_SECRET_ACCESS_KEY'],
        }
    else:
        kwargs = {}

    return storage.new_storage(storage_class_name, server_name, **kwargs)


def new_sign_url_from_config(sign_url_class_name, server_name, config):
    if sign_url_class_name == 'GoogleCloudStorageSignUrl':
        kwargs = {
            'bucket_name': config['GCS_BUCKET_NAME'],
        }
        if 'GC_JSON_CREDENTIALS_PATH' in config:
            kwargs['json_credentials_path'] = config['GC_JSON_CREDENTIALS_PATH']
        if 'GC_JSON_CREDENTIALS_STRING' in config:
            kwargs['json_credentials_string'] = config['GC_JSON_CREDENTIALS_STRING']
    elif sign_url_class_name == 'AmazonCloudFrontSignUrl':
        kwargs = {
            'domain_name': config['AWS_CF_DOMAIN_NAME'],
            'key_pair_id': config['AWS_CF_KEY_PAIR_ID'],
            'private_key_string': config['AWS_CF_PRIVATE_KEY_STRING'],
        }
    else:
        kwargs = {}

    return sign_url.new_sign_url(sign_url_class_name, server_name, **kwargs)

