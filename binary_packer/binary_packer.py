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


def unpack(bytes_):
    is_iterating = [False]
    def unpacked_byte_iter():
        for i in six.moves.xrange(size):
            yield __int_to_byte_func(next(byte_iter_obj))
        is_iterating[0] = False

    byte_iter_obj = iter(bytes_)
    while True:
        size = vlq.vlq_to_int(byte_iter_obj)
        if size is None:
            break
        is_iterating[0] = True
        yield unpacked_byte_iter()
        if is_iterating[0]:
            raise RuntimeError('begin new iteraton before last iteration finished')


def pack_from_file(fp_iter):
    return pack(map(lambda fp: (rest_file_size(fp), byte_iter(fp)), fp_iter))


def unpack_from_file(fp):
    return unpack(byte_iter(fp))


def pack_from_bytes(bytes_iter):
    return b''.join(pack(map(lambda bytes_: (len(bytes_), bytes_), bytes_iter)))


def unpack_from_bytes(bytes_):
    return tuple(map(lambda unpacked_byte_iter: b''.join(unpacked_byte_iter), unpack(bytes_)))


# test case

import unittest
import io


class BinaryPackerTestCase(unittest.TestCase):
    def pack_unpack_test(self, is_fp, is_from):
        origin_bytes_list = []
        item_list = []
        for exp in six.moves.xrange(20):
            n = 2**exp
            for i in (n-1, n):
                origin_bytes = os.urandom(i)
                origin_bytes_list.append(origin_bytes)
                if is_fp:
                    fp = io.BytesIO(origin_bytes)
                    if is_from:
                        item = fp
                    else:
                        size = rest_file_size(fp)
                        byte_iter_obj = byte_iter(fp)
                        item = (size, byte_iter_obj)
                else:
                    if is_from:
                        item = origin_bytes
                    else:
                        size = len(origin_bytes)
                        item = (size, iter(origin_bytes))
                item_list.append(item)

        if is_fp:
            packed_stream = io.BytesIO()
            if is_from:
                packed_byte_iter = pack_from_file(iter(item_list))
            else:
                packed_byte_iter = pack(iter(item_list))
            for b in packed_byte_iter:
                packed_stream.write(b)
        else:
            if is_from:
                packed_bytes = pack_from_bytes(iter(item_list))
            else:
                packed_bytearray = bytearray()
                packed_byte_iter = pack(iter(item_list))
                for b in packed_byte_iter:
                    packed_bytearray.append(vlq.byte_to_int(b))
                packed_bytes = bytes(packed_bytearray)

        if is_fp:
            packed_stream.seek(0)
            if is_from:
                unpack_iter = unpack_from_file(packed_stream)
            else:
                byte_iter_obj = byte_iter(packed_stream)
                unpack_iter = unpack(byte_iter_obj)
        else:
            if is_from:
                unpack_iter = unpack_from_bytes(packed_bytes)
            else:
                unpack_iter = unpack(iter(packed_bytes))
        origin_bytes_iter = iter(origin_bytes_list)

        for unpacked_byte_iter in unpack_iter:
            if is_fp:
                unpacked_stream = io.BytesIO()
                for b in unpacked_byte_iter:
                    unpacked_stream.write(b)
                unpacked_bytes = unpacked_stream.getvalue()
            else:
                if is_from:
                    unpacked_bytes = unpacked_byte_iter
                else:
                    unpacked_bytearray = bytearray()
                    for b in unpacked_byte_iter:
                        unpacked_bytearray.append(vlq.byte_to_int(b))
                    unpacked_bytes = bytes(unpacked_bytearray)

            origin_bytes = next(origin_bytes_iter)
            self.assertEqual(unpacked_bytes, origin_bytes, 'unpacked data is wrong')

    def test_pack_unpack_from_fp(self):
        self.pack_unpack_test(True, True)

    def test_pack_unpack_from_bytes(self):
        self.pack_unpack_test(False, True)

    def test_pack_unpack_fp(self):
        self.pack_unpack_test(True, False)

    def test_pack_unpack_bytes(self):
        self.pack_unpack_test(False, False)


if __name__ == '__main__':
    unittest.main()

