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


import gcloud.credentials
import boto.cloudfront.distribution
import rsa # module boto need module rsa

from . import helper


# sign url offline
class SignUrl(helper.KeyNameMixin):
    def generate_download_url(self, name_tuple, duration):
        raise NotImplementedError('generate_download_url() is not implemented')


class GoogleCloudStorageSignUrl(SignUrl):
    def __init__(self, server_name, bucket_name, json_credentials_path=None, json_credentials_string=None):
        super(GoogleCloudStorageSignUrl, self).__init__(server_name)

        if json_credentials_path is not None:
            self.credentials = gcloud.credentials.get_for_service_account_json(json_credentials_path)
        elif json_credentials_string is not None:
            self.credentials = helper.create_service_account_credentials_from_json(json_credentials_string)
        else:
            raise ValueError('must assign json_credential_path or json_credential_string')

        self.bucket_name = bucket_name

    def generate_download_url(self, name_tuple, duration):
        name = self._get_key_name(name_tuple)
        return helper.google_cloud_storage_generate_download_url(self.credentials, self.bucket_name, name, duration)


class AmazonCloudFrontSignUrl(SignUrl):
    def __init__(self, server_name, domain_name, key_pair_id, private_key_string):
        super(AmazonCloudFrontSignUrl, self).__init__(server_name)
        self.distribution = boto.cloudfront.distribution.Distribution(domain_name=domain_name)
        self.key_pair_id = key_pair_id
        self.private_key_string = private_key_string

    def generate_download_url(self, name_tuple, duration):
        name = self._get_key_name(name_tuple)
        return helper.amazon_cloudfront_generate_download_url(self.distribution, self.key_pair_id, self.private_key_string,
            name, duration)


def new_sign_url(sign_url_class_name, *args, **kwargs):
    sign_url_class = eval(sign_url_class_name)
    if issubclass(sign_url_class, SignUrl):
        return sign_url_class(*args, **kwargs)
    else:
        raise TypeError('must be name of subclass of SignUrl')

