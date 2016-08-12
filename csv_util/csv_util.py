#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# csv_util - csv reader and writer which accept unicode string items
#            compatible with Python 2 & 3
# Copyright (C) 2016-present Himawari Tachibana <fieliapm@gmail.com>
#               original source: https://docs.python.org/2/library/csv.html
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
import codecs
import io
import csv


def UTF8RecoderGenerator(fp, encoding):
    reader = codecs.getreader(encoding)(fp)
    while True:
        try:
            next_string = next(reader)
        except StopIteration:
            break
        else:
            if sys.version_info[0] < 3:
                next_string = next_string.encode('utf-8')
            yield next_string


class UTF8RecoderObject(object):
    def __init__(self, fp, encoding):
        self.reader = codecs.getreader(encoding)(fp)

    def __next(self):
        next_string = next(self.reader)
        if sys.version_info[0] < 3:
            next_string = next_string.encode('utf-8')
        return next_string

    def __next_generator(self):
        while True:
            try:
                next_item = self.__next()
            except StopIteration:
                break
            else:
                yield next_item

    def __iter__(self):
        return self.__next_generator()

    #if sys.version_info[0] < 3:
    #    next = __next
    #else:
    #    __next__ = __next


UTF8Recoder = UTF8RecoderGenerator
#UTF8Recoder = UTF8RecoderObject


class CSVUnicodeReader(object):
    def __init__(self, fp, dialect=csv.excel, encoding='utf-8', **kwargs):
        recoder = UTF8Recoder(fp, encoding)
        self.reader = csv.reader(recoder, dialect=dialect, **kwargs)

    @property
    def dialect(self):
        return self.reader.dialect

    @property
    def line_num(self):
        return self.reader.line_num

    def __next(self):
        row = next(self.reader)
        if sys.version_info[0] < 3:
            row = list(map(lambda item: item.decode('utf-8'), row))
        return row

    def __next_generator(self):
        while True:
            try:
                next_item = self.__next()
            except StopIteration:
                break
            else:
                yield next_item

    def __iter__(self):
        return self.__next_generator()

    #if sys.version_info[0] < 3:
    #    next = __next
    #else:
    #    __next__ = __next


class CSVUnicodeWriter(object):
    def __init__(self, fp, dialect=csv.excel, encoding='utf-8', **kwargs):
        if sys.version_info[0] < 3:
            self.queue = io.BytesIO()
        else:
            self.queue = io.StringIO(newline='')
        self.writer = csv.writer(self.queue, dialect=dialect, **kwargs)
        self.stream = fp
        self.encoder = codecs.getincrementalencoder(encoding)()

    @property
    def dialect(self):
        return self.writer.dialect

    def writerow(self, row):
        if sys.version_info[0] < 3:
            row = list(map(lambda item: item.encode('utf-8'), row))
        self.writer.writerow(row)
        data = self.queue.getvalue()
        if sys.version_info[0] < 3:
            data = data.decode('utf-8')
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.seek(0)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# test case

import unittest


class CSVUnicodeTestCase(unittest.TestCase):
    def setUp(self):
        self.__TEST_DATA = [
            [u'E6新幹線 "こまち"', u'Japan', u'電車', u'320.0'],
            [u'Infiniti G35 (V35), by Nissan', u'', u'自動車', u'250.0'],
        ]
        self.__TEST_FILE = '/tmp/csv_test.csv'

    def tearDown(self):
        pass

    def __is_iterable(self, obj):
        return callable(getattr(obj, '__iter__'))

    def __is_iterator(self, obj):
        if sys.version_info[0] < 3:
            status = callable(getattr(obj, 'next', None)) and not hasattr(obj, '__next__')
        else:
            status = callable(getattr(obj, '__next__', None)) and not hasattr(obj, 'next')
        return status

    def __check_attr(self, encoding):
        with open(self.__TEST_FILE, 'rb') as fp:
            recoder = UTF8Recoder(fp, encoding)
            self.assertTrue(self.__is_iterable(recoder), 'UTF8Recoder is not iterable')
            self.assertTrue(self.__is_iterator(iter(recoder)), 'iter(UTF8Recoder) is not iterator')
        with open(self.__TEST_FILE, 'rb') as fp:
            csv_reader = CSVUnicodeReader(fp, encoding=encoding)
            self.assertTrue(self.__is_iterable(csv_reader), 'CSVUnicodeReader is not iterable')
            self.assertTrue(self.__is_iterator(iter(csv_reader)), 'iter(CSVUnicodeReader) is not iterator')

    def __write_csv(self, encoding, use_writerows):
        with open(self.__TEST_FILE, 'wb') as fp:
            csv_writer = CSVUnicodeWriter(fp, encoding=encoding)
            print(repr(csv_writer.dialect))
            if bool(use_writerows):
                csv_writer.writerows(self.__TEST_DATA)
            else:
                for row in self.__TEST_DATA:
                    csv_writer.writerow(row)

    def __read_csv(self, encoding):
        with open(self.__TEST_FILE, 'rb') as fp:
            csv_reader = CSVUnicodeReader(fp, encoding=encoding)
            print(repr(csv_reader.dialect))
            i = 0
            self.assertEqual(csv_reader.line_num, i, 'line_num is wrong')
            for row_from_csv in csv_reader:
                row_from_test_data = self.__TEST_DATA[i]
                print(row_from_csv)
                print(row_from_test_data)
                self.assertEqual(row_from_csv, row_from_test_data, 'csv data is not equal to original data')
                i += 1
                self.assertEqual(csv_reader.line_num, i, 'line_num is wrong')
            self.assertEqual(i, len(self.__TEST_DATA), 'csv data is lost')

    def __test_one_codec(self, encoding):
        self.__write_csv(encoding, False)
        self.__read_csv(encoding)
        self.__write_csv(encoding, True)
        self.__read_csv(encoding)
        self.__check_attr(encoding)

    def test_codec_utf_8(self):
        self.__test_one_codec('utf-8')

    def _test_codec_shift_jis(self):
        self.__test_one_codec('shift-jis')

    def _test_codec_big5hkscs(self):
        self.__test_one_codec('big5hkscs')


if __name__ == '__main__':
    unittest.main()

