#!/usr/bin/env python
# -*- coding: utf-8 -*-


################################################################################
#
# redis_leaderboard - read and store leaderboard data with redis database
#                     compatible with Python 2 & 3
# Copyright (C) 2016-present Himawari Tachibana <fieliapm@gmail.com>
#               original source: https://docs.python.org/2/library/csv.html
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


import time
#import sys

import redis
import redis.sentinel
import rediscluster


def to_lex(big_integer):
    hex_string = '%x' % (big_integer,)
    hex_len = len(hex_string)
    return '%016x#%s' % (hex_len, hex_string)


class Leaderboard(object):
    @staticmethod
    def __get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name):
        leaderboard_name = ':'.join(leaderboard_name_tuple)
        return 'score:%s:%s:set' % (leaderboard_name, leaderboard_interval_name)

    @staticmethod
    def __get_score_detail_key_prefix(leaderboard_name_tuple, leaderboard_interval_name):
        leaderboard_name = ':'.join(leaderboard_name_tuple)
        return 'score:%s:%s:detail' % (leaderboard_name, leaderboard_interval_name)

    @staticmethod
    def __get_score_detail_key_timestamp(score_detail_key_prefix, user_id):
        return '%s:%s:%s' % (score_detail_key_prefix, user_id, 'timestamp')

    @staticmethod
    def __get_score_detail_key_info(score_detail_key_prefix, user_id):
        return '%s:%s:%s' % (score_detail_key_prefix, user_id, 'info')

    @staticmethod
    def __get_set_member_key(user_id, timestamp):
        timestamp_str = to_lex(int(timestamp))
        return '%s:%s' % (timestamp_str, user_id)

    def __init__(self, strict_redis):
        self.strict_redis = strict_redis

    def delete_high_score(self, leaderboard_name_tuple, leaderboard_interval_name, user_id):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)
        score_detail_key_prefix = self.__class__.__get_score_detail_key_prefix(leaderboard_name_tuple, leaderboard_interval_name)

        score_detail_key_timestamp = self.__class__.__get_score_detail_key_timestamp(score_detail_key_prefix, user_id)
        score_detail_key_info = self.__class__.__get_score_detail_key_info(score_detail_key_prefix, user_id)

        def delete_high_score_transaction(pipe):
            old_timestamp = pipe.get(score_detail_key_timestamp)
            if old_timestamp is not None:
                old_member_key = self.__class__.__get_set_member_key(user_id, old_timestamp)
                pipe.multi()
                pipe.zrem(score_set_key, old_member_key)
                pipe.delete(score_detail_key_timestamp)
                pipe.delete(score_detail_key_info)
                pipe.execute()

        self.strict_redis.transaction(delete_high_score_transaction, score_detail_key_timestamp, score_detail_key_info)

    def update_high_score(self, leaderboard_name_tuple, leaderboard_interval_name, user_id, score, timestamp, info):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)
        score_detail_key_prefix = self.__class__.__get_score_detail_key_prefix(leaderboard_name_tuple, leaderboard_interval_name)

        score_detail_key_timestamp = self.__class__.__get_score_detail_key_timestamp(score_detail_key_prefix, user_id)
        score_detail_key_info = self.__class__.__get_score_detail_key_info(score_detail_key_prefix, user_id)

        def update_high_score_transaction(pipe):
            #print('watch')
            #sys.stdin.readline()
            old_timestamp = pipe.get(score_detail_key_timestamp)
            if old_timestamp is not None:
                old_member_key = self.__class__.__get_set_member_key(user_id, old_timestamp)
                old_score = pipe.zscore(score_set_key, old_member_key)
            #print('get')
            #sys.stdin.readline()
            if old_timestamp is None or old_score <= score:
                new_member_key = self.__class__.__get_set_member_key(user_id, timestamp)
                pipe.multi()
                if old_timestamp is not None:
                    pipe.zrem(score_set_key, old_member_key)
                pipe.mset({score_detail_key_timestamp: timestamp, score_detail_key_info: info})
                pipe.zadd(score_set_key, score, new_member_key)
                pipe.execute()

        #print('before')
        #sys.stdin.readline()
        self.strict_redis.transaction(update_high_score_transaction, score_detail_key_timestamp, score_detail_key_info)
        #print('fin')


    def __attach_score_detail(self, leaderboard_name_tuple, leaderboard_interval_name, member_key_score_pair_list):
        score_detail_key_prefix = self.__class__.__get_score_detail_key_prefix(leaderboard_name_tuple, leaderboard_interval_name)

        if len(member_key_score_pair_list) > 0:
            (member_key_list, score_list) = zip(*member_key_score_pair_list)
            user_id_list = map(lambda member_key: member_key.split(':', 1)[1], member_key_list)
            field_key_info_list = map(lambda user_id: self.__class__.__get_score_detail_key_info(score_detail_key_prefix, user_id), user_id_list)
            info_list = self.strict_redis.mget(*field_key_info_list)
            result = list(zip(user_id_list, score_list, info_list))
        else:
            result = []
        return result

    def list_high_score(self, leaderboard_name_tuple, leaderboard_interval_name, start, num):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)

        member_key_score_pair_list = self.strict_redis.zrevrange(score_set_key, start, start+num-1, withscores=True)
        return self.__attach_score_detail(leaderboard_name_tuple, leaderboard_interval_name, member_key_score_pair_list)

    def list_high_score_reverse(self, leaderboard_name_tuple, leaderboard_interval_name, start, num):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)

        member_key_score_pair_list = self.strict_redis.zrange(score_set_key, start, start+num-1, withscores=True)
        return self.__attach_score_detail(leaderboard_name_tuple, leaderboard_interval_name, member_key_score_pair_list)

    def list_high_score_in_score_range(self, leaderboard_name_tuple, leaderboard_interval_name, max_score, min_score, start=None, num=None):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)

        member_key_score_pair_list = self.strict_redis.zrevrangebyscore(score_set_key, max_score, min_score, start=start, num=num, withscores=True)
        return self.__attach_score_detail(leaderboard_name_tuple, leaderboard_interval_name, member_key_score_pair_list)

    def list_high_score_reverse_in_score_range(self, leaderboard_name_tuple, leaderboard_interval_name, min_score, max_score, start=None, num=None):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)

        member_key_score_pair_list = self.strict_redis.zrangebyscore(score_set_key, min_score, max_score, start=start, num=num, withscores=True)
        return self.__attach_score_detail(leaderboard_name_tuple, leaderboard_interval_name, member_key_score_pair_list)

    def __get_rank(self, is_reverse, leaderboard_name_tuple, leaderboard_interval_name, user_id):
        score_set_key = self.__class__.__get_score_set_key(leaderboard_name_tuple, leaderboard_interval_name)
        score_detail_key_prefix = self.__class__.__get_score_detail_key_prefix(leaderboard_name_tuple, leaderboard_interval_name)

        score_detail_key_timestamp = self.__class__.__get_score_detail_key_timestamp(score_detail_key_prefix, user_id)
        score_detail_key_info = self.__class__.__get_score_detail_key_info(score_detail_key_prefix, user_id)

        (timestamp, info) = self.strict_redis.mget(score_detail_key_timestamp, score_detail_key_info)
        if timestamp is None:
            rank = None
            score = None
        else:
            member_key = self.__class__.__get_set_member_key(user_id, timestamp)
            if bool(is_reverse):
                rank = self.strict_redis.zrank(score_set_key, member_key)
            else:
                rank = self.strict_redis.zrevrank(score_set_key, member_key)
            score = self.strict_redis.zscore(score_set_key, member_key)
        return (rank, score, info)

    def get_rank(self, leaderboard_name_tuple, leaderboard_interval_name, user_id):
        return self.__get_rank(False, leaderboard_name_tuple, leaderboard_interval_name, user_id)

    def get_rank_reverse(self, leaderboard_name_tuple, leaderboard_interval_name, user_id):
        return self.__get_rank(True, leaderboard_name_tuple, leaderboard_interval_name, user_id)


# test case

import unittest


class LeaderboardTestCase(unittest.TestCase):
    def setUp(self):
        self.strict_redis = redis.StrictRedis(db=10)
        self.strict_redis.flushdb()
        self.leaderboard = Leaderboard(self.strict_redis)

    def tearDown(self):
        self.strict_redis.flushdb()


    def test_single_user_id(self):
        timestamp = int(time.time())

        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia', 900004.0, timestamp, 'fielia, score: 900004')
        self.leaderboard.delete_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia', 900000.0, timestamp, 'fielia, score: 900000')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia', 900002.0, timestamp, 'fielia, score: 900002')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia', 900001.0, timestamp, 'fielia, score: 900001')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'himawari', 900002.0, timestamp+1, 'himawari, score: 900002')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'new_user', 900003.0, timestamp-1, 'new_user, score: 900003')
        self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'extra'), 'all', 'new_user_2', 900005.0, timestamp-2, 'new_user_2, score: 900005')

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia')
        duration = time.time()-start_time
        self.assertEqual(rank, 2, 'fielia rank error')
        self.assertEqual(score, 900002.0, 'fielia score error')
        print(duration)
        print(info)

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank_reverse(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia')
        duration = time.time()-start_time
        self.assertEqual(rank, 0, 'fielia rank error')
        self.assertEqual(score, 900002.0, 'fielia score error')
        print(duration)
        print(info)

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'himawari')
        duration = time.time()-start_time
        self.assertEqual(rank, 1, 'himawari rank error')
        self.assertEqual(score, 900002.0, 'himawari score error')
        print(duration)
        print(info)

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank_reverse(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'himawari')
        duration = time.time()-start_time
        self.assertEqual(rank, 1, 'himawari rank error')
        self.assertEqual(score, 900002.0, 'himawari score error')
        print(duration)
        print(info)

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'new_user')
        duration = time.time()-start_time
        self.assertEqual(rank, 0, 'new_user rank error')
        self.assertEqual(score, 900003.0, 'new_user score error')
        print(duration)
        print(info)

        start_time = time.time()
        (rank, score, info) = self.leaderboard.get_rank_reverse(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'new_user')
        duration = time.time()-start_time
        self.assertEqual(rank, 2, 'new_user rank error')
        self.assertEqual(score, 900003.0, 'new_user score error')
        print(duration)
        print(info)

    def test_multiple_user_id(self):
        self.leaderboard = Leaderboard(self.strict_redis)
        timestamp = int(time.time())

        for i in range(1000000):
            score = float(i)
            self.leaderboard.update_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 'fielia%d' % (i,), score, timestamp, 'fielia, score: %f' % (score,))
            if i%1000==0:
                print(i)

        start_time = time.time()
        high_score_list = self.leaderboard.list_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 1, 5)
        duration = time.time()-start_time
        print(duration)
        #print(high_score_list)

        start_time = time.time()
        high_score_list = self.leaderboard.list_high_score_reverse(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 1, 5)
        duration = time.time()-start_time
        print(duration)
        #print(high_score_list)

        start_time = time.time()
        high_score_list = self.leaderboard.list_high_score_reverse(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 900001, 900000)
        duration = time.time()-start_time
        print(duration)
        #print(high_score_list)

        start_time = time.time()
        high_score_list = self.leaderboard.list_high_score_reverse_in_score_range(('race', 'nuerburgring-nordschleife', 'hard'),'all', 900000, 900001)
        duration = time.time()-start_time
        print(duration)
        #print(high_score_list)

        start_time = time.time()
        high_score_list = self.leaderboard.list_high_score(('race', 'nuerburgring-nordschleife', 'hard'), 'all', 500000, 100000)
        duration = time.time()-start_time
        print(duration)
        #print(high_score_list)


if __name__ == '__main__':
    unittest.main()

