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
import tempfile
import unittest
import unittest.mock

from parameterized import parameterized

from o2a.converter.exceptions import ParseException
from o2a.utils import el_utils
from o2a.utils.el_utils import normalize_path, escape_string_with_python_escapes, replace_url_el

# pylint: disable=too-many-public-methods
from o2a.o2a_libs.property_utils import PropertySet


class TestELUtils(unittest.TestCase):
    def test_parse_els_no_file(self):
        expected_properties = {}
        props = PropertySet(job_properties={"key": "value"}, config={}, action_node_properties={})
        self.assertEqual(expected_properties, el_utils.extract_evaluate_properties(None, props=props))

    def test_parse_els_file(self):
        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write("#comment\n" "key=value")
        prop_file.close()

        job_properties = {"test": "answer"}
        props = PropertySet(job_properties=job_properties, config={}, action_node_properties={})
        expected = {"key": "value"}
        self.assertEqual(expected, el_utils.extract_evaluate_properties(prop_file.name, props=props))

    def test_parse_els_file_list(self):
        # Should remain unchanged, as the conversion from a comma-separated string to a List will
        # occur before writing to file.
        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write("#comment\n" "key=value,value2,${test}")
        prop_file.close()

        job_properties = {"test": "answer"}
        props = PropertySet(config={}, job_properties=job_properties, action_node_properties={})
        expected = {"key": "value,value2,answer"}
        self.assertEqual(expected, el_utils.extract_evaluate_properties(prop_file.name, props=props))

    def test_parse_els_multiple_line_with_back_references(self):
        # Should remain unchanged, as the conversion from a comma-separated string to a List will
        # occur before writing to file.
        prop_file = tempfile.NamedTemporaryFile("w", delete=False)
        prop_file.write(
            """
#comment
key=value,value2,${test}
key2=value
key3=refer${key2}
key4=refer${key5}
key5=test
"""
        )
        prop_file.close()

        job_properties = {"test": "answer"}
        props = PropertySet(config={}, job_properties=job_properties, action_node_properties={})
        expected = {
            "key": "value,value2,answer",
            "key2": "value",
            "key3": "refervalue",
            "key4": "refer${key5}",  # no forward-references
            "key5": "test",
        }
        self.assertEqual(expected, el_utils.extract_evaluate_properties(prop_file.name, props=props))

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
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        result = normalize_path(oozie_path, props=PropertySet(config=config, job_properties=job_properties))
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
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        result = normalize_path(
            oozie_path, props=PropertySet(config=config, job_properties=job_properties), allow_no_schema=True
        )
        self.assertEqual(expected_result, result)

    @parameterized.expand(
        [
            ("${nameNode_1}/examples/output-data/demo/pig-node",),
            ("/examples/output-data/demo/pig-node",),
            ("http:///examples/output-data/demo/pig-node2",),
        ]
    )
    def test_normalize_path_red_path(self, oozie_path):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        with self.assertRaisesRegex(ParseException, "Unknown path format. "):
            normalize_path(oozie_path, props=PropertySet(config=config, job_properties=job_properties))

    @parameterized.expand(
        [("http:///examples/output-data/demo/pig-node2",), ("ftp:///examples/output-data/demo/pig-node2",)]
    )
    def test_normalize_path_red_path_allowed_no_schema(self, oozie_path):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        with self.assertRaisesRegex(ParseException, "Unknown path format. "):
            normalize_path(
                oozie_path,
                props=PropertySet(config=config, job_properties=job_properties),
                allow_no_schema=True,
            )

    @parameterized.expand(
        [
            (
                "${nameNode}/examples/output-data/demo/pig-node",
                "{{nameNode}}/examples/output-data/demo/pig-node",
            ),
            (
                "${nameNode}/examples/output-data/demo/pig-node2",
                "{{nameNode}}/examples/output-data/demo/pig-node2",
            ),
            ("hdfs:///examples/output-data/demo/pig-node2", "hdfs:///examples/output-data/demo/pig-node2"),
        ]
    )
    def test_replace_url_el_green_path(self, oozie_url, expected_result):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        result = replace_url_el(oozie_url, props=PropertySet(config=config, job_properties=job_properties))
        self.assertEqual(expected_result, result)

    @parameterized.expand(
        [
            ("test", "'test'"),
            ("ą", "'\\xc4\\x85'"),
            ("'", "'\\''"),
            (
                "This string is \" replaced with 'Escaped one'",
                "'This string is \" replaced with \\'Escaped one\\''",
            ),
            ('"', "'\"'"),
        ]
    )
    def test_escape_python_string(self, input_string, expected_string):
        self.assertEqual(expected_string, escape_string_with_python_escapes(input_string))
