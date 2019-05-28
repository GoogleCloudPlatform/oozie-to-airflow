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
"""Tests Variable Name"""
from unittest import TestCase

from parameterized import parameterized

from o2a.utils.variable_name_utils import convert_to_python_variable


class ConvertToPythonVariableTestCase(TestCase):
    def test_should_keep_original_when_its_not_required(self):
        self.assertEqual(convert_to_python_variable("TEST"), "TEST")

    @parameterized.expand(
        [
            ("TEST$TEST", "TEST_TEST"),
            ("TEST TEST", "TEST_TEST"),
            ('TEST"TEST', "TEST_TEST"),
            ("TEST'TEST", "TEST_TEST"),
            ("TEST'", "TEST_"),
        ]
    )
    def test_should_replace_invalid_characters_to_underscore(self, input_text, expected_output):
        self.assertEqual(convert_to_python_variable(input_text), expected_output)

    def test_should_remove_leading_invalid_characters(self):
        self.assertEqual(convert_to_python_variable("123123TEST'TEST"), "TEST_TEST")
