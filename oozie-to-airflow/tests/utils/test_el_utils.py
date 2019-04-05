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
"""Tests EL utils"""
import unittest
import unittest.mock
from utils import el_utils


class TestELUtils(unittest.TestCase):
    def test_strip_el(self):
        exp_func = 'concat("abc", "def")'
        exp_var = "hostname"

        el_func1 = '${concat("abc", "def")}'
        el_var1 = "${hostname}"
        el_func2 = '${ concat("abc", "def") }'
        el_var2 = "${ hostname }"

        self.assertEqual(el_utils.strip_el(el_func1), exp_func)
        self.assertEqual(el_utils.strip_el(el_func2), exp_func)
        self.assertEqual(el_utils.strip_el(el_var1), exp_var)
        self.assertEqual(el_utils.strip_el(el_var2), exp_var)

    def test_replace_el_with_var_var_no_quote(self):
        params = {"hostname": "airflow@apache.org"}
        el_var = "${hostname}"
        expected = "airflow@apache.org"

        replaced = el_utils.replace_el_with_var(el_var, params, quote=False)
        self.assertEqual(replaced, expected)

    def test_replace_el_with_var_func_no_quote(self):
        # functions shouldn't be replaced
        params = {}
        el_var = '${concat("abc", "def")}'
        expected = '${concat("abc", "def")}'

        replaced = el_utils.replace_el_with_var(el_var, params, quote=False)
        self.assertEqual(replaced, expected)

    def test_replace_el_with_var_var_quote(self):
        params = {"hostname": "airflow@apache.org"}
        el_var = "${hostname}"
        expected = "'airflow@apache.org'"

        replaced = el_utils.replace_el_with_var(el_var, params, quote=True)
        self.assertEqual(replaced, expected)

    def test_replace_el_with_var_func_quote(self):
        # functions shouldn't be replaced
        params = {}
        el_var = '${concat("abc", "def")}'
        expected = '\'${concat("abc", "def")}\''

        replaced = el_utils.replace_el_with_var(el_var, params, quote=True)
        self.assertEqual(replaced, expected)

    def test_parse_el_func(self):
        test_module = unittest.mock.Mock()
        test_module.__name__ = "test"

        el_func_map = {"concat": test_module}
        el_func1 = '${concat("as", "df")}'
        el_func2 = "${concat()}"
        expected1 = 'test("as", "df")'
        expected2 = "test()"

        self.assertEqual(expected1, el_utils.parse_el_func(el_func1, el_func_map))
        self.assertEqual(expected2, el_utils.parse_el_func(el_func2, el_func_map))

    def test_parse_el_func_none(self):
        el_func_map = {}
        test_str1 = "asdf"
        test_str2 = "${hostname}"

        self.assertEqual(el_utils.parse_el_func(test_str1, el_func_map), None)
        self.assertEqual(el_utils.parse_el_func(test_str2, el_func_map), None)

    def test_parse_el_func_fail(self):
        el_func_map = {}
        el_func = '${concat("abc, "def")}'

        with self.assertRaises(KeyError):
            el_utils.parse_el_func(el_func, el_func_map)

    def test_convert_el_to_jinja_var_no_quote(self):
        el_function = "${hostname}"
        expected = "{{ params.hostname }}"
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=False))

    def test_convert_el_to_jinja_var_quote(self):
        el_function = "${hostname}"
        expected = "'{{ params.hostname }}'"
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=True))

    def test_convert_el_to_jinja_func_no_quote(self):
        el_function = '${concat("ab", "de")}'
        expected = 'concat("ab", "de")'
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=False))

    def test_convert_el_to_jinja_func_quote(self):
        el_function = '${concat("ab", "de")}'
        expected = 'concat("ab", "de")'
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=True))

    def test_convert_el_to_jinja_no_change_no_quote(self):
        el_function = "no_el_here"
        expected = "no_el_here"
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=False))

    def test_convert_el_to_jinja_no_change_quote(self):
        el_function = "no_el_here"
        expected = "'no_el_here'"
        self.assertEqual(expected, el_utils.convert_el_to_jinja(el_function, quote=True))

    def test_parse_els_no_file(self):
        params = {"test": "answer"}
        self.assertEqual(params, el_utils.parse_els(None, params))

    def test_parse_els_file(self):
        import tempfile

        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write("#comment\n" "key=value")
        prop_file.close()

        params = {"test": "answer"}
        expected = {"test": "answer", "key": "value"}
        self.assertEqual(expected, el_utils.parse_els(prop_file.name, params))
