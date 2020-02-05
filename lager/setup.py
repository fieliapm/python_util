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


from distutils.core import setup


setup(name='lager',
    version='0.1.0',
    description='an easy tool to access cloud storage service,\nand also, abstract and simplify corresponding API',
    author='Himawari Tachibana',
    author_email='fieliapm@gmail.com',
    url='https://github.com/fieliapm/python_util',
    packages=['lager'],
    install_requires=[
        # for generate_signed_url()
        #'google-cloud<0.34.0',
        #'google-cloud-storage<1.15.0',
        # for generate_signed_url_v2() and generate_signed_url_v4()
        'google-cloud-storage>=1.15.0',
        'six',
        # boto develop can resolve "On Python installs built against OpenSSL 1.1.1, Boto fails with SNI errors"
        #'boto>=2.40.0', # support python3
        'boto @ git+https://github.com/boto/boto.git@91ba037e54ef521c379263b0ac769c66182527d7#egg=boto',
        'rsa',
        'gcs-oauth2-boto-plugin>=2.0', # support python3
    ],
)
