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


import sys
import struct


def int_to_byte(number):
    return struct.pack('B', number)


def byte_to_int(b):
    return struct.unpack('B', b)[0]


def int_to_vlq(number):
    vlq_byte_array = bytearray()
    i = number&0x7f
    vlq_byte_array.append(i)
    number = number>>7
    while number > 0:
        i = number&0x7f|0x80
        vlq_byte_array.append(i)
        number = number>>7
    vlq_byte_array.reverse()
    return bytes(vlq_byte_array)


if sys.version_info[0] < 3:
    __byte_to_int_func = byte_to_int
else:
    __byte_to_int_func = lambda i: i

def vlq_to_int(vlq_bytes_iter):
    number = 0
    for b in vlq_bytes_iter:
        i = __byte_to_int_func(b)
        number = (number<<7)|(i&0x7f)
        if not i>>7:
            return number
    return None


# test case

import unittest
import six.moves


class VLQTestCase(unittest.TestCase):
    def test_single_number(self):
        TEST_CASE = (
            (0x0, b'\x00'),
            (0x7f, b'\x7f'),
            (0x80, b'\x81\x00'),
            (0x3fff, b'\xff\x7f'),
            (0x4000, b'\x81\x80\x00'),
            (0x1fffff, b'\xff\xff\x7f'),
            (0x200000, b'\x81\x80\x80\x00'),
            (0xfffffff, b'\xff\xff\xff\x7f'),
            (0x10000000, b'\x81\x80\x80\x80\x00'),
            (0x7ffffffff, b'\xff\xff\xff\xff\x7f'),
            (0x800000000, b'\x81\x80\x80\x80\x80\x00'),
            (0x3ffffffffff, b'\xff\xff\xff\xff\xff\x7f'),
            (0x40000000000, b'\x81\x80\x80\x80\x80\x80\x00'),
            (0x1ffffffffffff, b'\xff\xff\xff\xff\xff\xff\x7f'),
            (0x2000000000000, b'\x81\x80\x80\x80\x80\x80\x80\x00'),
            (0xffffffffffffff, b'\xff\xff\xff\xff\xff\xff\xff\x7f'),
            (0x100000000000000, b'\x81\x80\x80\x80\x80\x80\x80\x80\x00'),
            (0x7fffffffffffffff, b'\xff\xff\xff\xff\xff\xff\xff\xff\x7f'),
        )

        for (n, vlq_bytes) in TEST_CASE:
            self.assertEqual(n, vlq_to_int(vlq_bytes), 'vlq_to_int(%s) failed' % (repr(vlq_bytes),))
            self.assertEqual(vlq_bytes, int_to_vlq(n), 'int_to_vlq(0x%x) failed' % (n,))

    def test_behavior(self):
        TEST_CASE = (
            (None, b''),
            (None, b'\x80'),
            (None, b'\xff'),
            (None, b'\x80\x80'),
            (None, b'\xff\xff'),
        )

        for (n, vlq_bytes) in TEST_CASE:
            self.assertEqual(n, vlq_to_int(vlq_bytes), 'vlq_to_int(%s) failed' % (repr(vlq_bytes),))

    def verify_one_number(self, n):
            self.assertEqual(n, vlq_to_int(int_to_vlq(n)), '0x%x failed' % (n,))

    def verify_two_number(self, n):
            n1 = n-1
            self.verify_one_number(n1)
            self.verify_one_number(n)

    def test_critical_int_number(self):
        for exp in six.moves.xrange(1000):
            self.verify_two_number(2**(exp*7))

    def test_all_int_number(self):
        for exp in six.moves.xrange(1000):
            self.verify_two_number(2**exp)


if __name__ == '__main__':
    unittest.main()

