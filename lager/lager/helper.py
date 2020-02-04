#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# lager - an easy tool to access cloud storage service,
#         and also, abstract and simplify corresponding API
# Copyright (C) 2015-present Himawari Tachibana <fieliapm@gmail.com>
#
# This file is part of lager
#
# lager is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
################################################################################


import time
import base64
import binascii
import json

import six.moves.urllib.parse
import google.cloud.storage._signing
import google.cloud.storage.blob
import google.oauth2.service_account


def get_key_name(server_name, name_tuple):
    return '/'.join(type(name_tuple)((server_name,))+name_tuple).encode('utf-8')


def get_name_tuple(server_name, key_name):
    internal_name_tuple = tuple(key_name.split('/'))
    if internal_name_tuple[0] == server_name:
        return internal_name_tuple[1:]
    else:
        raise ValueError('key name is not belong to server name')


def expire_time(duration):
    return int(time.time())+duration


def base64_to_hex(base64data):
    return binascii.hexlify(base64.standard_b64decode(base64data)).decode('utf-8')


class KeyNameMixin(object):
    def _get_key_name(self, name_tuple):
        return get_key_name(self.server_name, name_tuple)

    def __init__(self, server_name):
        self.server_name = server_name


# for Google Cloud
def create_service_account_credentials_from_json(json_credentials_string):
    json_credentials_data = json.loads(json_credentials_string)
    return google.oauth2.service_account.Credentials.from_service_account_info(json_credentials_data)


# for Google Cloud

def __google_cloud_storage_resource_path(bucket_name, name):
    return '/{bucket_name}/{quoted_name}'.format(
        bucket_name=bucket_name,
        quoted_name=six.moves.urllib.parse.quote(name, safe=''))


def google_cloud_storage_public_download_url(bucket_name, name):
    resource = __google_cloud_storage_resource_path(bucket_name, name)
    return '{endpoint}{resource}'.format(
        endpoint=google.cloud.storage.blob._API_ACCESS_ENDPOINT,
        resource=resource)


def google_cloud_storage_generate_download_url(credentials, bucket_name, name, duration):
    resource = __google_cloud_storage_resource_path(bucket_name, name)

    # generate_signed_url() need google-cloud<0.34.0 or google-cloud-storage<1.15.0
    # generate_signed_url_v2() and generate_signed_url_v4() need google-cloud-storage>=1.15.0

    #return google.cloud.storage._signing.generate_signed_url(credentials, resource, expire_time(duration),
    return google.cloud.storage._signing.generate_signed_url_v2(credentials, resource, expire_time(duration),
    #return google.cloud.storage._signing.generate_signed_url_v4(credentials, resource, duration,
        api_access_endpoint=google.cloud.storage.blob._API_ACCESS_ENDPOINT, method='GET')


# for Amazon CloudFront

def amazon_cloudfront_public_download_url(distribution, name):
    return 'https://%s/%s' % (distribution.domain_name, six.moves.urllib.parse.quote(name))


def amazon_cloudfront_generate_download_url(distribution, key_pair_id, private_key_string, name, duration):
    url = amazon_cloudfront_public_download_url(distribution, name)
    return distribution.create_signed_url(url, key_pair_id,
        expire_time=expire_time(duration), private_key_string=private_key_string)

