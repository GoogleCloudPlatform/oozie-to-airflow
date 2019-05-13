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

from parameterized import parameterized

from converter.exceptions import ParseException
from utils import el_utils
from utils.el_utils import normalize_path_by_adding_hdfs_if_needed, escape_string_with_python_escapes


# pylint: disable=too-many-public-methods
class TestELUtils(unittest.TestCase):
    def test_replace_el_with_var_property(self):
        properties = {"hostname": "airflow@apache.org"}
        el_var = "${hostname}"
        expected = "airflow@apache.org"

        replaced = el_utils.replace_el_with_property_values(el_var, properties)
        self.assertEqual(replaced, expected)

    def test_no_replace_el_func_with_property_values(self):
        # functions shouldn't be replaced
        properties = {}
        el_var = '${concat("abc", "def")}'
        expected = '${concat("abc", "def")}'

        replaced = el_utils.replace_el_with_property_values(el_var, properties)
        self.assertEqual(replaced, expected)

    def test_no_replace_wf_el_func_with_property_values(self):
        # functions shouldn't be replaced
        properties = {}
        el_var = '${wf:conf("abc", "def")}'
        expected = '${wf:conf("abc", "def")}'

        replaced = el_utils.replace_el_with_property_values(el_var, properties)
        self.assertEqual(replaced, expected)

    @parameterized.expand(
        [
            ('${concat("as", "df")}', '{el_basic_functions.test("as", "df")}'),
            ("${concat()}", "{el_basic_functions.test()}"),
            ("${concat('as', 'df')}", '{el_basic_functions.test("as", "df")}'),
        ]
    )
    def test_parse_el_func(self, input_string, expected_string):
        test_module = unittest.mock.Mock()
        test_module.__name__ = "test"

        el_func_map = {"concat": test_module}
        self.assertEqual(
            expected_string,
            el_utils.fstring_for_first_el_function(
                el_string=input_string, properties={}, func_map=el_func_map
            ),
        )

    @parameterized.expand(
        [
            ('${wf:conf("as", "df")}', '{el_wf_functions.test(CTX, "as", "df")}'),
            ("${wf:conf()}", "{el_wf_functions.test(CTX)}"),
            ("${wf:conf('as', 'df')}", '{el_wf_functions.test(CTX, "as", "df")}'),
        ]
    )
    def test_parse_wf_el_func(self, input_string, expected_string):
        test_module = unittest.mock.Mock()
        test_module.__name__ = "test"

        el_wf_func_map = {"wf:conf": test_module}
        self.assertEqual(
            expected_string,
            el_utils.fstring_for_first_wf_el_function(
                el_string=input_string, properties={}, func_map=el_wf_func_map
            ),
        )

    @parameterized.expand([("asdf",), ("${hostname}",)])
    def test_parse_el_func_none(self, input_string):
        el_func_map = {}

        self.assertIsNone(
            el_utils.fstring_for_first_el_function(
                el_string=input_string, properties={}, func_map=el_func_map
            )
        )

    def test_parse_el_func_fail(self):
        el_func_map = {}
        el_func = '${concat("abc, "def")}'

        with self.assertRaises(KeyError):
            el_utils.fstring_for_first_el_function(el_string=el_func, properties={}, func_map=el_func_map)

    def test_parse_wf_el_func_fail(self):
        el_func_map = {}
        el_func = '${wf:conf("abc, "def")}'

        with self.assertRaises(KeyError):
            el_utils.fstring_for_first_wf_el_function(el_string=el_func, properties={}, func_map=el_func_map)

    def test_convert_el_to_string_property(self):
        el_function = "${hostname}"
        expected = '{CTX["hostname"]}'
        self.assertEqual(
            expected,
            el_utils.convert_el_string_to_fstring(el_string=el_function, properties={"hostname": "host"}),
        )

    def test_convert_el_to_string_property_missing_property(self):
        el_function = "${hostname}"
        with self.assertRaises(KeyError):
            el_utils.convert_el_string_to_fstring(el_string=el_function, properties={})

    def test_convert_el_to_string_simple_function(self):
        el_function = '${concat("ab", "de")}'
        expected = '{el_basic_functions.concat("ab", "de")}'
        self.assertEqual(
            expected, el_utils.convert_el_string_to_fstring(el_string=el_function, properties={})
        )

    def test_convert_el_to_string_wf_conf(self):
        el_function = '${wf:conf("user.name")}'
        expected = '{el_wf_functions.wf_conf(CTX, "user.name")}'
        self.assertEqual(
            expected, el_utils.convert_el_string_to_fstring(el_string=el_function, properties={})
        )

    def test_convert_el_to_string_no_quotes(self):
        el_function = "no_el_here but"
        expected = "no_el_here but"
        self.assertEqual(expected, el_utils.convert_el_string_to_fstring(el_function, properties={}))

    def test_convert_el_to_string_quote_single(self):
        el_function = "no_el_here but '"
        expected = "no_el_here but \\'"
        self.assertEqual(expected, el_utils.convert_el_string_to_fstring(el_function, properties={}))

    def test_convert_el_to_string_quote_double(self):
        el_function = 'no_el_here but "'
        expected = 'no_el_here but "'
        self.assertEqual(expected, el_utils.convert_el_string_to_fstring(el_function, properties={}))

    def test_convert_el_to_string_multiple_constructs(self):
        el_function = (
            "This is ${hostname} together with ${concat('a','b')} and"
            " ${wf:conf('a')} repeated several times  "
            "  ${another} together with ${firstNotNull('a','b')} and ${wf:conf('a')}"
        )
        expected = (
            'This is {CTX["hostname"]} together with '
            '{el_basic_functions.concat("a","b")} and {el_wf_functions.wf_conf(CTX, "a")} '
            'repeated several times    {CTX["another"]} together '
            'with {el_basic_functions.first_not_null("a","b")} and '
            '{el_wf_functions.wf_conf(CTX, "a")}'
        )
        self.assertEqual(
            expected,
            el_utils.convert_el_string_to_fstring(
                el_function, properties={"hostname": "host", "another": "test"}
            ),
        )

    def test_parse_els_no_file(self):
        properties = {"test": "answer"}
        self.assertEqual(properties, el_utils.parse_el_property_file_into_dictionary(None, properties))

    def test_parse_els_file(self):
        import tempfile

        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write("#comment\n" "key=value")
        prop_file.close()

        properties = {"test": "answer"}
        expected = {"test": "answer", "key": "value"}
        self.assertEqual(
            expected, el_utils.parse_el_property_file_into_dictionary(prop_file.name, properties)
        )

    def test_parse_els_file_with_properties(self):
        import tempfile

        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write("#comment\n" "key=value\n" "\n" "key2=key2-${test}-postfix\n" "key3=key-3-${key2}\n")
        prop_file.close()

        properties = {"test": "answer"}
        expected = {
            "test": "answer",
            "key": "value",
            "key2": "key2-answer-postfix",
            "key3": "key-3-key2-answer-postfix",
        }
        self.assertEqual(
            expected, el_utils.parse_el_property_file_into_dictionary(prop_file.name, properties)
        )

    @parameterized.expand(
        [
            ("${nameNode}/examples/output-data/demo/pig-node", "/examples/output-data/demo/pig-node"),
            ("${nameNode}/examples/output-data/demo/pig-node2", "/examples/output-data/demo/pig-node2"),
            ("hdfs:///examples/output-data/demo/pig-node2", "/examples/output-data/demo/pig-node2"),
        ]
    )
    def test_normalize_path_green_path(self, oozie_path, expected_result):
        cluster = "my-cluster"
        region = "europe-west3"
        properties = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        result = normalize_path_by_adding_hdfs_if_needed(oozie_path, properties)
        self.assertEqual(expected_result, result)

    @parameterized.expand(
        [
            ("${nameNode}/examples/output-data/demo/pig-node", "/examples/output-data/demo/pig-node"),
            ("${nameNode}/examples/output-data/demo/pig-node2", "/examples/output-data/demo/pig-node2"),
            ("hdfs:///examples/output-data/demo/pig-node2", "/examples/output-data/demo/pig-node2"),
            ("/examples/output-data/demo/pig-node", "/examples/output-data/demo/pig-node"),
        ]
    )
    def test_normalize_path_with_allow_no_schema(self, oozie_path, expected_result):
        cluster = "my-cluster"
        region = "europe-west3"
        properties = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        result = normalize_path_by_adding_hdfs_if_needed(oozie_path, properties, allow_no_schema=True)
        self.assertEqual(expected_result, result)

    @parameterized.expand(
        [("/examples/output-data/demo/pig-node",), ("http:///examples/output-data/demo/pig-node2",)]
    )
    def test_normalize_path_red_path(self, oozie_path):
        cluster = "my-cluster"
        region = "europe-west3"
        properties = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        with self.assertRaisesRegex(ParseException, "Unknown path format. "):
            normalize_path_by_adding_hdfs_if_needed(oozie_path, properties)

    @parameterized.expand([("${nameNodeAAA}/examples/output-data/demo/pig-node",)])
    def test_normalize_path_red_path2(self, oozie_path):
        cluster = "my-cluster"
        region = "europe-west3"
        properties = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        with self.assertRaisesRegex(Exception, "The property nameNodeAAA cannot be found in"):
            normalize_path_by_adding_hdfs_if_needed(oozie_path, properties)

    @parameterized.expand(
        [("http:///examples/output-data/demo/pig-node2",), ("ftp:///examples/output-data/demo/pig-node2",)]
    )
    def test_normalize_path_red_path_allowed_no_schema(self, oozie_path):
        cluster = "my-cluster"
        region = "europe-west3"
        properties = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        with self.assertRaisesRegex(ParseException, "Unknown path format. "):
            normalize_path_by_adding_hdfs_if_needed(oozie_path, properties, allow_no_schema=True)

    @parameterized.expand(
        [
            ("test", "test"),
            ("ą", "\\xc4\\x85"),
            ("'", "\\'"),
            (
                "This string is \" replaced with 'Escaped one'",
                "This string is \" replaced with \\'Escaped one\\'",
            ),
            ('"', '"'),
        ]
    )
    def test_escape_python_code(self, input_string, expected_string):
        self.assertEqual(expected_string, escape_string_with_python_escapes(input_string))
