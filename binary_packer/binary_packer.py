#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# binary_packer - pack/unpack multiple binary
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


import sys
import os
import six.moves

import vlq


if sys.version_info[0] < 3:
    __int_to_byte_func = lambda b: b
else:
    __int_to_byte_func = vlq.int_to_byte


def rest_file_size(fp):
    start_pos = fp.tell()
    fp.seek(0, os.SEEK_END)
    end_pos = fp.tell()
    fp.seek(start_pos)
    return end_pos-start_pos


def byte_iter(fp, buffer_size=None):
    if buffer_size is None:
        buffer_size=16*1024
    while True:
        buf = fp.read(buffer_size)
        if not buf:
            break
        for b in buf:
            yield b


def pack(size_bytes_pair_iter):
    for (size, bytes_) in size_bytes_pair_iter:
        vlq_bytes = vlq.int_to_vlq(size)
        for b in vlq_bytes:
            yield __int_to_byte_func(b)
        byte_iter_obj = iter(bytes_)
        for i in six.moves.xrange(size):
            yield __int_to_byte_func(next(byte_iter_obj))


def unpack(bytes_, buffer_size=None):
    is_iterating = [False]
    def unpack_iter():
        for i in six.moves.xrange(size):
            yield __int_to_byte_func(next(byte_iter_obj))
        is_iterating[0] = False

    byte_iter_obj = iter(bytes_)
    while True:
        size = vlq.vlq_to_int(byte_iter_obj)
        if size is None:
            break
        is_iterating[0] = True
        yield unpack_iter()
        if is_iterating[0]:
            raise RuntimeError('begin new iteraton before last iteration finished')


# test case

import unittest
import io


class BinaryPackerTestCase(unittest.TestCase):
    def pack_unpack_test(self, is_fp):
        origin_bytes_list = []
        size_bytes_pair_list = []
        for exp in six.moves.xrange(20):
            n = 2**exp
            for i in (n-1, n):
                origin_bytes = os.urandom(i)
                origin_bytes_list.append(origin_bytes)
                if is_fp:
                    fp = io.BytesIO(origin_bytes)
                    size = rest_file_size(fp)
                    byte_iter_obj = byte_iter(fp)
                else:
                    size = len(origin_bytes)
                    byte_iter_obj = origin_bytes
                size_bytes_pair_list.append((size, byte_iter_obj))

        if is_fp:
            packed_stream = io.BytesIO()
        else:
            packed_bytearray = bytearray()
        for b in pack(iter(size_bytes_pair_list)):
            if is_fp:
                packed_stream.write(b)
            else:
                packed_bytearray.append(vlq.byte_to_int(b))

        if is_fp:
            packed_stream.seek(0)
            byte_iter_obj = byte_iter(packed_stream)
        else:
            byte_iter_obj = bytes(packed_bytearray)
        origin_bytes_iter = iter(origin_bytes_list)
        for unpack_iter in unpack(byte_iter_obj):
            if is_fp:
                unpacked_stream = io.BytesIO()
            else:
                unpacked_bytearray = bytearray()
            for b in unpack_iter:
                if is_fp:
                    unpacked_stream.write(b)
                else:
                    unpacked_bytearray.append(vlq.byte_to_int(b))
            if is_fp:
                unpacked_bytes = unpacked_stream.getvalue()
            else:
                unpacked_bytes = bytes(unpacked_bytearray)
            origin_bytes = next(origin_bytes_iter)
            self.assertEqual(unpacked_bytes, origin_bytes, 'unpacked data is wrong')

    def test_pack_unpack_bytes(self):
        self.pack_unpack_test(False)

    def test_pack_unpack_fp(self):
        self.pack_unpack_test(True)


if __name__ == '__main__':
    unittest.main()

