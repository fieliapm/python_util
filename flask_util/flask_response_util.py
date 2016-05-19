#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# flask_response_util - flask response utility
# Copyright (C) 2016-present Himawari Tachibana <fieliapm@gmail.com>
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


from functools import wraps

import flask
import werkzeug.http


def add_response_headers(headers={}):
    '''This decorator adds the headers passed in to the response'''
    def decorator(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            response = flask.make_response(func(*args, **kwargs))
            original_headers = response.headers
            for (header, value) in headers.items():
                original_headers[header] = value
            original_headers.setdefault('Date', werkzeug.http.http_date())
            return response
        return decorated_function
    return decorator


def cache_control(second):
    '''This decorator passes cache control'''
    return add_response_headers({'Cache-Control': 'public,s-maxage=%d' % (second,)})

