#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# vlq - convert between bigint and variable-length quantity
# Copyright (C) 2015-present Himawari Tachibana <fieliapm@gmail.com>
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


setup(name='vlq',
    version='0.1.0',
    description='convert between bigint and variable-length quantity',
    author='Himawari Tachibana',
    author_email='fieliapm@gmail.com',
    url='https://github.com/fieliapm/python_util',
    py_modules=['vlq'],
)
