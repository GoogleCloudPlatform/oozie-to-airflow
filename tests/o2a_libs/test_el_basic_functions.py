# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for all EL basic functions"""
import unittest

from parameterized import parameterized

from o2a.o2a_libs.el_basic_functions import first_not_null, concat, replace_all, append_all, trim, url_encode


class TestElBasicFunctions(unittest.TestCase):
    @parameterized.expand(
        [
            ("foo", "bar", "foo"),
            ("foo", "", "foo"),
            ("foo", None, "foo"),
            ("", "bar", "bar"),
            (None, "bar", "bar"),
            ("", "", ""),
            (None, None, ""),
        ]
    )
    def test_first_not_null(self, str_one, str_two, expected):
        self.assertEqual(expected, first_not_null(str_one, str_two))

    @parameterized.expand(
        [
            ("foo", "bar", "foobar"),
            ("foo", "", "foo"),
            ("foo", None, "foo"),
            ("", "bar", "bar"),
            (None, "bar", "bar"),
        ]
    )
    def test_concat(self, str_one, str_two, expected):
        self.assertEqual(expected, concat(str_one, str_two))

    @parameterized.expand(
        [
            ("foobar", "bar", "rab", "foorab"),
            ("foobar", "", "rab", "foobar"),
            ("foobar", None, "rab", "foobar"),
            ("foobar", "bar", "", "foo"),
            ("foobar", "bar", None, "foo"),
            ("foobar", "[o]", "a", "faabar"),
        ]
    )
    def test_replace_all(self, src_str, regex, replacement, expected):
        self.assertEqual(expected, replace_all(src_str, regex, replacement))

    @parameterized.expand(
        [
            ("/a/b/,/c/b/,/c/d/", "ADD", ",", "/a/b/ADD,/c/b/ADD,/c/d/ADD"),
            ("/a/b/,/c/b/,/c/d/", "", ",", "/a/b/,/c/b/,/c/d/"),
            ("/a/b/,/c/b/,/c/d/", None, ",", "/a/b/,/c/b/,/c/d/"),
            ("/a/b/,/c/b/,/c/d/", "ADD", "", "/a/b/,/c/b/,/c/d/"),
            ("/a/b/,/c/b/,/c/d/", "ADD", None, "/a/b/,/c/b/,/c/d/"),
        ]
    )
    def test_append_all(self, src_str, append, delimiter, expected):
        self.assertEqual(expected, append_all(src_str, append, delimiter))

    @parameterized.expand([("foo", "foo"), ("  foo ", "foo"), ("\n  foo \t", "foo"), ("", ""), (None, "")])
    def test_trim(self, src_str, expected):
        self.assertEqual(expected, trim(src_str))

    @parameterized.expand([(" ", "%20"), ("?", "%3F"), ("", ""), (None, "")])
    def test_urlencode(self, src_str, expected):
        self.assertEqual(expected, url_encode(src_str))
