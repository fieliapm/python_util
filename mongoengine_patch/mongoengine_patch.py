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


import warnings

import mongoengine
import pymongo


def _does_query_include_index(query, index_spec):
    for (field, order) in index_spec['fields']:
        if field not in query:
            return False
    return True


def _does_query_match_unique_document(query_set, query):
    for index_spec in query_set._document._meta['index_specs']:
        if 'unique' in index_spec and index_spec['unique']:
            if _does_query_include_index(query, index_spec):
                return True
    return False


def _get_or_create(query_set, query):
    # get default values from key 'defaults' and remove key 'defaults' from query
    defaults = query.pop('defaults', {})

    # check if this query includes any unique index
    if not _does_query_match_unique_document(query_set, query):
        warnings.warn('this query does not include any unique index', RuntimeWarning, stacklevel=3)

    # the value of a key in defaults have higher priority than the value of the same key in query
    create_kwargs = query.copy()
    create_kwargs.update(defaults)

    # if any required field is not in create_kwargs, add required field and its default value to upsert_dict
    upsert_doc = query_set._document(**create_kwargs)
    upsert_doc.validate()
    upsert_dict = upsert_doc.to_mongo().to_dict()

    modify_kwargs = {
        'upsert': True,
    }
    for key in upsert_dict:
        modify_kwargs['set_on_insert__' + key] = upsert_dict[key]

    return (upsert_doc, modify_kwargs)


def get_or_create_v2(self, **query):
    (upsert_doc, modify_kwargs) = _get_or_create(self, query)

    modify_kwargs['full_response'] = True
    modify_kwargs['new'] = True

    result = self(**query).modify(**modify_kwargs)
    upsert_doc = result['value']
    created = 'upserted' in result['lastErrorObject']

    return (upsert_doc, created)


def get_or_create_v3(self, **query):
    (upsert_doc, modify_kwargs) = _get_or_create(self, query)

    modify_kwargs['full_result'] = True

    result = self(**query).update(**modify_kwargs)
    if not isinstance(result, dict):
        result = result.raw_result
    #created = result.upserted_id is not None
    created = not result['updatedExisting']
    if created:
        #upsert_doc.id = result.upserted_id
        upsert_doc.id = result['upserted']
        upsert_doc._clear_changed_fields()
    else:
        upsert_doc = self.get(**query)

    return (upsert_doc, created)


# patch get_or_create() because it is not atomic
if pymongo.version_tuple[0] < 3:
    mongoengine.queryset.QuerySet.get_or_create = get_or_create_v2
else:
    mongoengine.queryset.QuerySet.get_or_create = get_or_create_v3


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
    meta = {
        'indexes': [
            {
                'fields': ['name', 'age'],
                'unique': True,
            },
            {
                'fields': ['sex'],
                'unique': False,
            },
            {
                'fields': ['job'],
                'unique': False,
            },
            {
                'fields': ['counter'],
                'unique': True,
            },
        ],
    }


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
            (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=17, defaults={'age': 18, 'network_info': {'blog': 'http://yaplog.jp/marinavi/', 'social_network': {'twitter': 'mari_navi'}}})

        counter = 1
        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=17, defaults={'age': 18, 'sex': 'female', 'network_info': {'blog': 'http://yaplog.jp/marinavi/', 'social_network': {'twitter': 'mari_navi'}}})
        self.assertTrue(is_created, 'it should be new contact')
        self.assertNotEqual(contact.id, None, 'id is None')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 1, 'counter is not 1')
        self.assertEqual(contact.network_info.blog, 'http://yaplog.jp/marinavi/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.facebook, None, 'facebook account is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, 'mari_navi', 'twitter account is wrong')
        self.assertEqual(contact.network_info.social_network.plurk, None, 'plurk account is wrong')

        counter = 2
        (contact, is_created) = Contact.objects.get_or_create(name=u'井上麻里奈', age=18, defaults={'age': 30, 'sex': 'male', 'network_info': {'blog': 'http://yaplog.jp/marinavi/', 'social_network': {'twitter': 'mari_navi'}}})
        self.assertFalse(is_created, 'it should be old contact')
        self.assertNotEqual(contact.id, None, 'id is None')
        self.assertEqual(contact.name, u'井上麻里奈', 'name is wrong')
        self.assertEqual(contact.age, 18, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 1, 'counter is not 1')
        self.assertEqual(contact.network_info.blog, 'http://yaplog.jp/marinavi/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.facebook, None, 'facebook account is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, 'mari_navi', 'twitter account is wrong')
        self.assertEqual(contact.network_info.social_network.plurk, None, 'plurk account is wrong')

        counter = 3
        (contact, is_created) = Contact.objects.get_or_create(name=u'林原めぐみ', age=47, defaults={'age': 48, 'sex': 'female', 'network_info': {'blog': 'http://ameblo.jp/megumi-hayashibara-hs/'}})
        self.assertTrue(is_created, 'it should be new contact')
        self.assertNotEqual(contact.id, None, 'id is None')
        self.assertEqual(contact.name, u'林原めぐみ', 'name is wrong')
        self.assertEqual(contact.age, 48, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 3, 'counter is not 3')
        self.assertEqual(contact.network_info.blog, 'http://ameblo.jp/megumi-hayashibara-hs/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.facebook, None, 'facebook account is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, None, 'twitter account is wrong')
        self.assertEqual(contact.network_info.social_network.plurk, None, 'plurk account is wrong')

        counter = 4
        (contact, is_created) = Contact.objects.get_or_create(name=u'林原めぐみ', age=48, defaults={'age': 49, 'sex': 'male', 'network_info': {'blog': 'http://ameblo.jp/megumi-hayashibara-hs/'}})
        self.assertFalse(is_created, 'it should be old contact')
        self.assertNotEqual(contact.id, None, 'id is None')
        self.assertEqual(contact.name, u'林原めぐみ', 'name is wrong')
        self.assertEqual(contact.age, 48, 'age is wrong')
        self.assertEqual(contact.sex, 'female', 'sex is wrong')
        self.assertEqual(contact.job, 'seiyuu', 'job is wrong')
        self.assertEqual(contact.counter, 3, 'counter is not 3')
        self.assertEqual(contact.network_info.blog, 'http://ameblo.jp/megumi-hayashibara-hs/', 'blog URL is wrong')
        self.assertEqual(contact.network_info.social_network.facebook, None, 'facebook account is wrong')
        self.assertEqual(contact.network_info.social_network.twitter, None, 'twitter account is wrong')
        self.assertEqual(contact.network_info.social_network.plurk, None, 'plurk account is wrong')


if __name__ == '__main__':
    unittest.main()

