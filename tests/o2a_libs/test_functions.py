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
"""Tests for all functions module"""

import unittest

from parameterized import parameterized

import o2a.o2a_libs.functions as functions
from o2a.o2a_libs.el_wf_functions import _reverse_task_map


class TestFunctions(unittest.TestCase):
    @parameterized.expand(
        [
            ("bar", "bar", "foo"),
            ("foo", "foo", ""),
            ("foo", None, "foo"),
            ("bar", "", "bar"),
            ("bar", None, "bar"),
            ("", "", ""),
            ("", None, None),
        ]
    )
    def test_first_not_null(self, expected, str_one, str_two):
        self.assertEqual(expected, functions.first_not_null(str_one, str_two))

    @parameterized.expand(
        [
            ("foo ~ bar", "foo", "bar"),
            ("foo", "foo", ""),
            ("foo", "foo", None),
            ("bar", "", "bar"),
            ("bar", None, "bar"),
            ("", None, None),
        ]
    )
    def test_concat(self, expected, str_one, str_two):
        self.assertEqual(expected, functions.concat(str_one, str_two))

    @parameterized.expand(
        [
            ("foorab", "foobar", "bar", "rab"),
            ("foobar", "foobar", "", "rab"),
            ("foobar", "foobar", None, "rab"),
            ("foo", "foobar", "bar", ""),
            ("foo", "foobar", "bar", None),
            ("faabar", "foobar", "[o]", "a"),
        ]
    )
    def test_replace_all(self, expected, src_str, regex, replacement):
        self.assertEqual(expected, functions.replace_all(src_str, regex, replacement))

    @parameterized.expand(
        [
            ("/a/b/ADD,/c/b/ADD,/c/d/ADD", "/a/b/,/c/b/,/c/d/", "ADD", ","),
            ("/a/b/,/c/b/,/c/d/", "/a/b/,/c/b/,/c/d/", "", ","),
            ("/a/b/,/c/b/,/c/d/", "/a/b/,/c/b/,/c/d/", None, ","),
            ("/a/b/,/c/b/,/c/d/", "/a/b/,/c/b/,/c/d/", "ADD", ""),
            ("/a/b/,/c/b/,/c/d/", "/a/b/,/c/b/,/c/d/", "ADD", None),
        ]
    )
    def test_append_all(self, expected, src_str, append, delimiter):
        self.assertEqual(expected, functions.append_all(src_str, append, delimiter))

    @parameterized.expand([("foo.strip()", "foo"), ("'  foo '.strip()", "'  foo '"), ("", None)])
    def test_trim(self, expected, src_str):
        self.assertEqual(expected, functions.trim(src_str))

    @parameterized.expand([("%20", " "), ("%3F", "?"), ("", ""), ("", None)])
    def test_urlencode(self, expected, src_str):
        self.assertEqual(expected, functions.url_encode(src_str))

    @parameterized.expand([('{"key": "value"}', {"key": "value"})])
    def test_to_json_str(self, expected, src_str):
        self.assertEqual(expected, functions.to_json_str(src_str))

    def test_reverse_map(self):
        props = {"key1": ["value1", "value2"], "key2": ["value3"]}
        keys = sum(props.values(), [])
        values = props.keys()

        rev = _reverse_task_map(props)
        for key in keys:
            self.assertIn(key, rev.keys())

        for val in values:
            self.assertIn(val, rev.values())
