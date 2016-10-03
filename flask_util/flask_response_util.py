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


import math
import time
from functools import wraps

import flask
import werkzeug.http


def fractional_part(x):
    return x-math.floor(x)


def round_maxage(current_unix_timestamp, second):
    return max(int(math.floor(fractional_part(current_unix_timestamp)+second)), 0)


def __add_cache_control_to_headers(headers, s_maxage):
    headers['Cache-Control'] = 'public,s-maxage=%d' % (s_maxage,)


def add_precise_cache_control_to_headers(headers, second):
    '''Add precise cache control s-maxage to headers.
    It accept second of type float.

    CAUTION:
    If we round up current unix timestamp and use it in request processing function,
    we must replace flask.g.current_unix_timestamp with rounded up timestamp before calling add_precise_cache_control_to_headers()
    and return point of request processing function.
    '''
    s_maxage = round_maxage(flask.g.current_unix_timestamp, second)
    __add_cache_control_to_headers(headers, s_maxage)


def template_response_headers(headers={}):
    '''This decorator attaches template headers to every response returned by request processing function.
    flask.g.current_unix_timestamp contains current UNIX timestamp.
    Because the floor of represented time stored in flask.g.current_unix_timestamp is same as represented time in HTTP header field "Date",
    we can tight our request processing logic with HTTP header field "Date".
    BTW, we can access flask.g.current_unix_timestamp in the body of decorated function.

    CAUTION:
    If we round up current unix timestamp and use it in request processing function,
    we must replace flask.g.current_unix_timestamp with rounded up timestamp before calling add_precise_cache_control_to_headers()
    and return point of request processing function.
    '''
    def decorator(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            flask.g.current_unix_timestamp = time.time()
            response = flask.make_response(func(*args, **kwargs))
            original_headers = response.headers
            for (header, value) in headers.items():
                original_headers.setdefault(header, value)
            original_headers.setdefault('Date', werkzeug.http.http_date(flask.g.current_unix_timestamp))
            original_headers.setdefault('Timestamp', repr(flask.g.current_unix_timestamp))
            return response
        return decorated_function
    return decorator


def cache_control(s_maxage):
    '''This decorator attaches predefined cache control to every response returned by request processing function.
    '''
    headers = {}
    __add_cache_control_to_headers(headers, s_maxage)
    return template_response_headers(headers)

