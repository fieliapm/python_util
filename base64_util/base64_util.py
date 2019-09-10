#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# base64_util - utility for handling different format of base64 encoded data
# Copyright (C) 2016-present Himawari Tachibana <fieliapm@gmail.com>
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


import base64


class Base64Codec(object):
    def __init__(self, encoder, decoder):
        self.__encoder = encoder
        self.__decoder = decoder

    def b64encode_bytes(self, bytes_):
        return self.__encoder(bytes_).decode('utf-8')

    def b64decode_bytes(self, encoded_bytes):
        return self.__decoder(encoded_bytes.encode('utf-8'))

    def b64encode_bytes_without_padding(self, bytes_):
        return self.b64encode_bytes(bytes_).rstrip('=')

    def b64decode_bytes_without_padding(self, encoded_bytes):
        base64_padding = '='*((-(len(encoded_bytes)&0x3))&0x3)
        return self.b64decode_bytes(encoded_bytes+base64_padding)

    def b64encode_string(self, string):
        return self.b64encode_bytes(string.encode('utf-8'))

    def b64decode_string(self, encoded_string):
        return self.b64decode_bytes(encoded_string).decode('utf-8')

    def b64encode_string_without_padding(self, string):
        return self.b64encode_bytes_without_padding(string.encode('utf-8'))

    def b64decode_string_without_padding(self, encoded_string):
        return self.b64decode_bytes_without_padding(encoded_string).decode('utf-8')


standard = Base64Codec(base64.standard_b64encode, base64.standard_b64decode)
urlsafe = Base64Codec(base64.urlsafe_b64encode, base64.urlsafe_b64decode)


# test case

import unittest


class Base64UtilTestCase(unittest.TestCase):
    def setUp(self):
        self.decoded_string = u'歪みねぇな!定岡ウェーブ'
        self.encoded_string = '5q2q44G/44Gt44GH44GqIeWumuWyoeOCpuOCp+ODvOODlg=='
        self.encoded_string_without_padding = self.encoded_string.rstrip('=')
        self.urlsafe_encoded_string = self.encoded_string.replace('+', '-').replace('/', '_')
        self.urlsafe_encoded_string_without_padding = self.urlsafe_encoded_string.rstrip('=')

    def test_standard_byte(self):
        self.assertEqual(standard.b64encode_bytes(self.decoded_string.encode('utf-8')), self.encoded_string)
        self.assertEqual(standard.b64decode_bytes(self.encoded_string), self.decoded_string.encode('utf-8'))

    def test_standard_byte_without_padding(self):
        self.assertEqual(standard.b64encode_bytes_without_padding(self.decoded_string.encode('utf-8')), self.encoded_string_without_padding)
        self.assertEqual(standard.b64decode_bytes_without_padding(self.encoded_string_without_padding), self.decoded_string.encode('utf-8'))

    def test_standard_string(self):
        self.assertEqual(standard.b64encode_string(self.decoded_string), self.encoded_string)
        self.assertEqual(standard.b64decode_string(self.encoded_string), self.decoded_string)

    def test_standard_string_without_padding(self):
        self.assertEqual(standard.b64encode_string_without_padding(self.decoded_string), self.encoded_string_without_padding)
        self.assertEqual(standard.b64decode_string_without_padding(self.encoded_string_without_padding), self.decoded_string)

    def test_urlsafe_byte(self):
        self.assertEqual(urlsafe.b64encode_bytes(self.decoded_string.encode('utf-8')), self.urlsafe_encoded_string)
        self.assertEqual(urlsafe.b64decode_bytes(self.urlsafe_encoded_string), self.decoded_string.encode('utf-8'))

    def test_urlsafe_byte_without_padding(self):
        self.assertEqual(urlsafe.b64encode_bytes_without_padding(self.decoded_string.encode('utf-8')), self.urlsafe_encoded_string_without_padding)
        self.assertEqual(urlsafe.b64decode_bytes_without_padding(self.urlsafe_encoded_string_without_padding), self.decoded_string.encode('utf-8'))

    def test_urlsafe_string(self):
        self.assertEqual(urlsafe.b64encode_string(self.decoded_string), self.urlsafe_encoded_string)
        self.assertEqual(urlsafe.b64decode_string(self.urlsafe_encoded_string), self.decoded_string)

    def test_urlsafe_string_without_padding(self):
        self.assertEqual(urlsafe.b64encode_string_without_padding(self.decoded_string), self.urlsafe_encoded_string_without_padding)
        self.assertEqual(urlsafe.b64decode_string_without_padding(self.urlsafe_encoded_string_without_padding), self.decoded_string)


if __name__ == '__main__':
    unittest.main()

