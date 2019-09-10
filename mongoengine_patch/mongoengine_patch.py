#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# mongoengine_patch - some patch for MongoEngine (with PyMongo 2.x)
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


import mongoengine
import pymongo


if pymongo.version_tuple[0] >= 3:
    raise ImportError('With PyMongo 3+, it is not possible anymore to implement get_or_create().')


# patch get_or_create() because it is not atomic

def get_or_create(self, **query):
    # get default values from key 'defaults' and remove keyword 'defaults' from query
    defaults = query.pop('defaults', {})
    # the value of a key in defaults have higher priority than the value of the same key in query
    for key in query:
        if key not in defaults:
            defaults[key] = query[key]
    # extract plain field in defaults
    defaults_plain_field_set = set(map(lambda key: key.split('__', 1)[0], defaults))
    # if any required field is not in defaults, add required field and its default value to defaults
    invalid_field_list = []
    for (field_name, field_obj) in self._document._fields.items():
        if field_name not in defaults_plain_field_set:
            if field_obj.default is None:
                if field_obj.required:
                    invalid_field_list.append(field_name)
            else:
                if hasattr(field_obj.default, '__call__'):
                    default_value = field_obj.default()
                else:
                    default_value = field_obj.default
                defaults[field_name] = default_value
    if len(invalid_field_list) > 0:
        raise mongoengine.errors.ValidationError('Field is required: %s' % (repr(invalid_field_list),))

    modify_dict = {
        'upsert': True,
        'full_response': True,
        'new': True,
    }
    for key in defaults:
        modify_dict['set_on_insert__' + key] = defaults[key]

    result = self(**query).modify(**modify_dict)

    return (result['value'], 'upserted' in result['lastErrorObject'])

mongoengine.queryset.QuerySet.get_or_create = get_or_create


# test case

import unittest


counter = 0
def get_counter():
    return counter


class SocialNetwork(mongoengine.document.EmbeddedDocument):
    facebook = mongoengine.fields.StringField()
    twitter = mongoengine.fields.StringField()
    plurk = mongoengine.fields.StringField()


class NetworkInfo(mongoengine.document.EmbeddedDocument):
    blog = mongoengine.fields.StringField()
    social_network = mongoengine.fields.EmbeddedDocumentField(SocialNetwork, default=SocialNetwork)


class Contact(mongoengine.document.Document):
    name = mongoengine.fields.StringField(required=True, default='')
    age = mongoengine.fields.LongField()
    sex = mongoengine.fields.StringField(required=True)
    job = mongoengine.fields.StringField(required=True, default='seiyuu')
    counter = mongoengine.fields.LongField(default=get_counter)
    network_info = mongoengine.fields.EmbeddedDocumentField(NetworkInfo, default=NetworkInfo)


class MongoEnginePatchTestCase(unittest.TestCase):
    def setUp(self):
        self.test_db_name = '_test_db'
        self.connection = mongoengine.connect(db=self.test_db_name)
        self.connection.drop_database(self.test_db_name)

    def tearDown(self):
        self.connection.drop_database(self.test_db_name)

    def test_get_or_create(self):
        global counter

        counter = 0
        with self.assertRaises(mongoengine.errors.ValidationError):
            (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=17, defaults={'age': 18, 'network_info__blog': 'http://yaplog.jp/marinavi/', 'network_info__social_network__twitter': 'mari_navi'})

        counter = 1
        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=17, defaults={'age': 18, 'sex': 'female', 'network_info__blog': 'http://yaplog.jp/marinavi/', 'network_info__social_network__twitter': 'mari_navi'})
        self.assertTrue(is_created, 'it should be new contact')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 1, 'counter is not 1')
        self.assertEqual(contact.network_info.blog, 'http://yaplog.jp/marinavi/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, 'mari_navi', 'twitter account is wrong')

        counter = 2
        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=18, defaults={'age': 30, 'sex': 'male', 'network_info__blog': 'http://yaplog.jp/navimari/', 'network_info__social_network__twitter': 'navi_mari'})
        self.assertFalse(is_created, 'it should be old contact')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 1, 'counter is not 1')
        self.assertEqual(contact.network_info.blog, 'http://yaplog.jp/marinavi/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, 'mari_navi', 'twitter account is wrong')

        counter = 3
        (contact, is_created) = Contact.objects.get_or_create(name=u'林原めぐみ', age=47, defaults={'age': 48, 'sex': 'female', 'network_info__blog': 'http://ameblo.jp/megumi-hayashibara-hs/'})
        self.assertTrue(is_created, 'it should be new contact')
        self.assertEqual(contact.name, u'林原めぐみ', 'name is wrong')
        self.assertEqual(contact.age, 48, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 3, 'counter is not 3')
        self.assertEqual(contact.network_info.blog, 'http://ameblo.jp/megumi-hayashibara-hs/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, None, 'twitter account is wrong')


if __name__ == '__main__':
    unittest.main()

