#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# redis_leaderboard - read and store leaderboard data with redis database
#                     compatible with Python 2 & 3
# Copyright (C) 2016-present Himawari Tachibana <fieliapm@gmail.com>
#               original source: https://docs.python.org/2/library/csv.html
#
# This program is free software: you can redistribute it and/or modify
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


setup(name='redis_leaderboard',
    version='0.1.0',
    description='read and store leaderboard data with redis database\ncompatible with Python 2 & 3',
    author='Himawari Tachibana',
    author_email='fieliapm@gmail.com',
    url='https://github.com/fieliapm/python_util',
    py_modules=['redis_leaderboard'],
)
