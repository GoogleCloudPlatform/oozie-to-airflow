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

from o2a.o2a_libs.el_basic_functions import first_not_null


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
