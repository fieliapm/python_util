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


import google.cloud.storage

import boto
import boto.cloudfront
import gcs_oauth2_boto_plugin
import rsa # module boto need module rsa

from . import helper


# In AWS CloudFront Distribution:
# Origin Settings: Restrict Bucket Access: Yes
#                  Grant Read Permissions on Bucket: Yes
# Default Cache Behavior Settings: Restrict Viewer Access (Use Signed URLs or Signed Cookies): Yes


MAX_FILES = int(2**31-1)


class Storage(helper.KeyNameMixin):
    def _get_key_prefix(self, name_tuple_prefix):
        return helper.get_key_name(self.server_name, name_tuple_prefix+type(name_tuple_prefix)(('',)))

    def _get_name_tuple(self, key_name):
        return helper.get_name_tuple(self.server_name, key_name)

    def list_file(self, name_tuple_prefix, max_files=None):
        raise NotImplementedError('list() is not implemented')

    def delete_file(self, name_tuple):
        raise NotImplementedError('delete_file() is not implemented')

    def copy_file(self, src_name_tuple, dst_name_tuple):
        raise NotImplementedError('copy_file() is not implemented')

    def upload_file(self, name_tuple, fp, content_type=None, size=None):
        raise NotImplementedError('upload_file() is not implemented')

    def download_file(self, name_tuple, fp):
        raise NotImplementedError('download_file() is not implemented')

    def set_file_access_control(self, name_tuple, is_public):
        raise NotImplementedError('set_file_access_control() is not implemented')

    def get_file_info(self, name_tuple):
        raise NotImplementedError('get_file_info() is not implemented')

    def generate_url(self, name_tuple, duration, method='GET'):
        raise NotImplementedError('generate_url() is not implemented')


class GoogleCloudStorage(Storage):
    def __init__(self, server_name, bucket_name, project, json_credentials_path=None, json_credentials_string=None):
        super(GoogleCloudStorage, self).__init__(server_name)

        if json_credentials_path is not None:
            self.client = google.cloud.storage.client.Client.from_service_account_json(json_credentials_path, project=project)
        elif json_credentials_string is not None:
            self.credentials = helper.create_service_account_credentials_from_json(json_credentials_string)
            self.client = google.cloud.storage.client.Client(project, self.credentials)
        else:
            raise ValueError('must assign json_credential_path or json_credential_string')
        self.bucket = self.client.get_bucket(bucket_name)

    def list_file(self, name_tuple_prefix, max_files=None):
        path_prefix = self._get_key_prefix(name_tuple_prefix)
        #TODO: Google Cloud Storage always lists all blobs even if maxResults is set
        blob_iter = self.bucket.list_blobs(max_results=max_files, prefix=path_prefix)
        return map(lambda blob: self._get_name_tuple(blob.name), blob_iter)

    def delete_file(self, name_tuple):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_blob(path) is not None:
            self.bucket.delete_blob(path)
            return True
        else:
            return False

    def copy_file(self, src_name_tuple, dst_name_tuple):
        src_path = self._get_key_name(src_name_tuple)
        dst_path = self._get_key_name(dst_name_tuple)
        src_blob = self.bucket.get_blob(src_path)
        if src_blob is not None:
            self.bucket.copy_blob(src_blob, self.bucket, dst_path)
            return True
        else:
            return False

    def upload_file(self, name_tuple, fp, content_type=None, size=None):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_blob(path) is None:
            blob = self.bucket.blob(path)
            blob.upload_from_file(fp, size=size, content_type=content_type)
            return True
        else:
            return False

    def download_file(self, name_tuple, fp):
        blob = self.bucket.get_blob(self._get_key_name(name_tuple))
        if blob is not None:
            blob.download_to_file(fp)
            return True
        else:
            return False

    def set_file_access_control(self, name_tuple, is_public):
        blob = self.bucket.get_blob(self._get_key_name(name_tuple))
        if blob is not None:
            if bool(is_public):
                blob.make_public()
            else:
                blob.acl.all().revoke_read()
                blob.acl.save()
            return True
        else:
            return False

    def get_file_info(self, name_tuple):
        blob = self.bucket.get_blob(self._get_key_name(name_tuple))
        if blob is None:
            info = None
        else:
            info = {
                'content_type': blob.content_type,
                'size': blob.size,
                'checksum': {
                    'md5': helper.base64_to_hex(blob.md5_hash),
                    'crc32': helper.base64_to_hex(blob.crc32c),
                },
            }
        return info

    def generate_url(self, name_tuple, duration, method='GET'):
        blob = self.bucket.get_blob(self._get_key_name(name_tuple))
        if blob is None:
            url = None
        else:
            url = blob.generate_signed_url(helper.expire_time(duration), method=method)
        return url


class GoogleCloudStorage_Boto(Storage):
    def __init__(self, server_name, bucket_name, gs_access_key_id=None, gs_secret_access_key=None, is_interoperability_mode=True):
        super(GoogleCloudStorage_Boto, self).__init__(server_name)
        if is_interoperability_mode:
            self.gs_connection = boto.connect_gs(gs_access_key_id, gs_secret_access_key)
            self.bucket = self.gs_connection.get_bucket(bucket_name)
        else:
            gcs_oauth2_boto_plugin.oauth2_helper.SetFallbackClientIdAndSecret(gs_access_key_id, gs_secret_access_key)
            self.gs_storage_uri = boto.storage_uri(bucket_name, 'gs')
            self.bucket = self.gs_storage_uri.get_bucket(bucket_name)

    def list_file(self, name_tuple_prefix, max_files=None):
        path_prefix = self._get_key_prefix(name_tuple_prefix)
        if max_files is None:
            max_files = MAX_FILES
        key_result_set = self.bucket.get_all_keys(max_keys=max_files, prefix=path_prefix)
        return map(lambda key: self._get_name_tuple(key.key), key_result_set)

    def delete_file(self, name_tuple):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_key(path) is not None:
            self.bucket.delete_key(path)
            return True
        else:
            return False

    def copy_file(self, src_name_tuple, dst_name_tuple):
        src_path = self._get_key_name(src_name_tuple)
        dst_path = self._get_key_name(dst_name_tuple)
        if self.bucket.get_key(src_path) is not None:
            self.bucket.copy_key(dst_path, self.bucket.name, src_path)
            return True
        else:
            return False

    def upload_file(self, name_tuple, fp, content_type=None, size=None):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_key(path) is None:
            key = self.bucket.new_key(path)
            headers = ({'Content-Type': content_type}, None)[content_type is None]
            key.set_contents_from_file(fp, headers=headers, size=size)
            return True
        else:
            return False

    def download_file(self, name_tuple, fp):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is not None:
            key.get_contents_to_file(fp)
            return True
        else:
            return False

    def set_file_access_control(self, name_tuple, is_public):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is not None:
            if bool(is_public):
                key.make_public()
            else:
                key.set_canned_acl('private')
            return True
        else:
            return False

    def get_file_info(self, name_tuple):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is None:
            info = None
        else:
            info = {
                'content_type': key.content_type,
                'size': key.size,
                'checksum': {
                    'md5': key.etag.strip('"'),
                },
            }
        return info

    def generate_url(self, name_tuple, duration, method='GET'):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is None:
            url = None
        else:
            url = key.generate_url(duration, method=method)
        return url


class AmazonS3Storage(Storage):
    def __init__(self, server_name, bucket_name, aws_access_key_id=None, aws_secret_access_key=None):
        super(AmazonS3Storage, self).__init__(server_name)
        self.s3_connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
        self.bucket = self.s3_connection.get_bucket(bucket_name)

    def list_file(self, name_tuple_prefix, max_files=None):
        path_prefix = self._get_key_prefix(name_tuple_prefix)
        if max_files is None:
            max_files = MAX_FILES
        key_result_set = self.bucket.get_all_keys(max_keys=max_files, prefix=path_prefix)
        return map(lambda key: self._get_name_tuple(key.key), key_result_set)

    def delete_file(self, name_tuple):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_key(path) is not None:
            self.bucket.delete_key(path)
            return True
        else:
            return False

    def copy_file(self, src_name_tuple, dst_name_tuple):
        src_path = self._get_key_name(src_name_tuple)
        dst_path = self._get_key_name(dst_name_tuple)
        if self.bucket.get_key(src_path) is not None:
            self.bucket.copy_key(dst_path, self.bucket.name, src_path)
            return True
        else:
            return False

    def upload_file(self, name_tuple, fp, content_type=None, size=None):
        path = self._get_key_name(name_tuple)
        if self.bucket.get_key(path) is None:
            key = self.bucket.new_key(path)
            headers = ({'Content-Type': content_type}, None)[content_type is None]
            key.set_contents_from_file(fp, headers=headers, size=size)
            return True
        else:
            return False

    def download_file(self, name_tuple, fp):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is not None:
            key.get_contents_to_file(fp)
            return True
        else:
            return False

    def set_file_access_control(self, name_tuple, is_public):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is not None:
            if bool(is_public):
                key.make_public()
            else:
                key.set_canned_acl('private')
            return True
        else:
            return False

    def get_file_info(self, name_tuple):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is None:
            info = None
        else:
            info = {
                'content_type': key.content_type,
                'size': key.size,
                'checksum': {
                    'md5': key.etag.strip('"'),
                },
            }
        return info

    def generate_url(self, name_tuple, duration, method='GET'):
        key = self.bucket.get_key(self._get_key_name(name_tuple))
        if key is None:
            url = None
        else:
            url = key.generate_url(duration, method=method)
        return url


class AmazonCloudFrontS3Storage(Storage):
    def __get_bucket(self):
        if isinstance(self.distribution.config.origin, boto.cloudfront.origin.S3Origin):
            bucket_dns_name = self.distribution.config.origin.dns_name
            bucket_name = bucket_dns_name.replace('.s3.amazonaws.com', '')

            bucket = self.s3_connection.get_bucket(bucket_name)
            return bucket
        else:
            raise NotImplementedError('Unable to get_objects on CustomOrigin')

    def __invalidate_file(self, path):
        self.cloudfront_connection.create_invalidation_request(self.distribution_id, [path])

    def __init__(self, server_name, distribution_id, key_pair_id, private_key_string, aws_access_key_id=None, aws_secret_access_key=None):
        super(AmazonCloudFrontS3Storage, self).__init__(server_name)

        self.cloudfront_connection = boto.connect_cloudfront(aws_access_key_id, aws_secret_access_key)
        self.s3_connection = boto.connect_s3(aws_access_key_id, aws_secret_access_key)

        self.distribution_id = distribution_id
        self.distribution = self.cloudfront_connection.get_distribution_info(self.distribution_id)
        self.key_pair_id = key_pair_id
        self.private_key_string = private_key_string

    def list_file(self, name_tuple_prefix, max_files=None):
        path_prefix = self._get_key_prefix(name_tuple_prefix)
        if max_files is None:
            max_files = MAX_FILES
        bucket = self.__get_bucket()
        key_result_set = bucket.get_all_keys(max_keys=max_files, prefix=path_prefix)
        return map(lambda key: self._get_name_tuple(key.key), key_result_set)

    def delete_file(self, name_tuple):
        path = self._get_key_name(name_tuple)
        bucket = self.__get_bucket()
        if bucket.get_key(path) is not None:
            bucket.delete_key(path)
            self.__invalidate_file(path)
            return True
        else:
            return False

    def copy_file(self, src_name_tuple, dst_name_tuple):
        src_path = self._get_key_name(src_name_tuple)
        dst_path = self._get_key_name(dst_name_tuple)
        bucket = self.__get_bucket()
        if bucket.get_key(src_path) is not None:
            bucket.copy_key(dst_path, bucket.name, src_path)
            return True
        else:
            return False

    def upload_file(self, name_tuple, fp, content_type=None, size=None):
        path = self._get_key_name(name_tuple)
        bucket = self.__get_bucket()
        if bucket.get_key(path) is None:
            headers = ({'Content-Type': content_type}, None)[content_type is None]
            self.distribution.add_object(path, fp, headers=headers, replace=False)
            self.__invalidate_file(path)
            return True
        else:
            return False

    def download_file(self, name_tuple, fp):
        path = self._get_key_name(name_tuple)
        bucket = self.__get_bucket()
        key = bucket.get_key(path)
        if key is not None:
            key.get_contents_to_file(fp)
            return True
        else:
            return False

    def set_file_access_control(self, name_tuple, is_public):
        path = self._get_key_name(name_tuple)
        bucket = self.__get_bucket()
        key = bucket.get_key(path)
        if key is not None:
            if bool(is_public):
                key.make_public()
            else:
                key.set_canned_acl('private')
            return True
        else:
            return False

    def get_file_info(self, name_tuple):
        path = self._get_key_name(name_tuple)
        bucket = self.__get_bucket()
        key = bucket.get_key(path)
        if key is None:
            info = None
        else:
            info = {
                'content_type': key.content_type,
                'size': key.size,
                'checksum': {
                    'md5': key.etag.strip('"'),
                },
            }
        return info

    def generate_url(self, name_tuple, duration, method='GET'):
        if method == 'GET':
            return helper.amazon_cloudfront_generate_download_url(self.distribution, self.key_pair_id, self.private_key_string,
                self._get_key_name(name_tuple), duration)
        else:
            raise ValueError('cloudfront support GET method only')


def new_storage(storage_class_name, *args, **kwargs):
    storage_class = eval(storage_class_name)
    if issubclass(storage_class, Storage):
        return storage_class(*args, **kwargs)
    else:
        raise TypeError('must be name of subclass of Storage')

