#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# mongoengine_patch - some patch for MongoEngine (with PyMongo 2.x)
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


import mongoengine
import pymongo


if pymongo.version_tuple[0] >= 3:
    raise ImportError('With PyMongo 3+, it is not possible anymore to implement get_or_create().')


# patch get_or_create() because it is not atomic

def get_or_create(self, **query):
    # get default values from key 'defaults' and remove keyword 'defaults' from query
    defaults = query.pop('defaults', {})

    modify_dict = {
        'upsert': True,
        'full_response': True,
        'new': True,
    }
    # the value of a key in defaults have higher priority than the value of the same key in query
    for key in query:
        modify_dict['set_on_insert__' + key] = query[key]
    for key in defaults:
        modify_dict['set_on_insert__' + key] = defaults[key]

    result = self(**query).modify(**modify_dict)

    return (result['value'], 'upserted' in result['lastErrorObject'])

mongoengine.queryset.QuerySet.get_or_create = get_or_create


# test case

import unittest

class Contact(mongoengine.document.Document):
    name = mongoengine.fields.StringField()
    age = mongoengine.fields.LongField()
    twitter = mongoengine.fields.StringField()
    blog = mongoengine.fields.StringField()


class MongoEnginePatchTestCase(unittest.TestCase):
    def setUp(self):
        self.test_db_name = '_test_db'
        self.connection = mongoengine.connect(db=self.test_db_name)
        self.connection.drop_database(self.test_db_name)

    def tearDown(self):
        self.connection.drop_database(self.test_db_name)

    def test_get_or_create(self):
        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=17, defaults={'age': 18, 'twitter': 'mari_navi', 'blog': 'http://yaplog.jp/marinavi/'})
        self.assertTrue(is_created, 'it should be new contact')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.twitter, 'mari_navi','twitter account is wrong')
        self.assertEqual(contact.blog, 'http://yaplog.jp/marinavi/', 'blog address is wrong')

        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=18, defaults={'age': 30, 'twitter': 'navi_mari', 'blog': 'http://yaplog.jp/navimari/'})
        self.assertFalse(is_created, 'it should be old contact')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.twitter, 'mari_navi','twitter account is wrong')
        self.assertEqual(contact.blog, 'http://yaplog.jp/marinavi/', 'blog address is wrong')


if __name__ == '__main__':
    unittest.main()

