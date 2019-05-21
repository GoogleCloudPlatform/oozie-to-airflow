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
"""Tests File mapper"""
import unittest
from xml.etree.ElementTree import Element
from xml.etree import ElementTree as ET

from o2a.utils.file_archive_extractors import FileExtractor


class TestFileExtractor(unittest.TestCase):
    def setUp(self):
        self.default_params = {
            "nameNode": "hdfs://",
            "oozie.wf.application.path": "hdfs:///user/pig/examples/pig_test_node",
        }

    def test_add_relative_file(self):
        # Given
        file_extractor = FileExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        file_extractor.add_file("test_file")
        # Then
        self.assertEqual(file_extractor.files, ["test_file"])
        self.assertEqual(file_extractor.hdfs_files, ["hdfs:///user/pig/examples/pig_test_node/test_file"])

    def test_add_absolute_file(self):
        # Given
        file_extractor = FileExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        file_extractor.add_file("/test_file")
        # Then
        self.assertEqual(file_extractor.files, ["/test_file"])
        self.assertEqual(file_extractor.hdfs_files, ["hdfs:///test_file"])

    def test_add_multiple_files(self):
        # Given
        file_extractor = FileExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        file_extractor.add_file("/test_file")
        file_extractor.add_file("test_file2")
        file_extractor.add_file("/test_file3")
        # Then
        self.assertEqual(file_extractor.files, ["/test_file", "test_file2", "/test_file3"])
        self.assertEqual(
            file_extractor.hdfs_files,
            ["hdfs:///test_file", "hdfs:///user/pig/examples/pig_test_node/test_file2", "hdfs:///test_file3"],
        )

    def test_add_hash_files(self):
        # Given
        file_extractor = FileExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        file_extractor.add_file("/test_file#test3_link")
        file_extractor.add_file("test_file2#test_link")
        file_extractor.add_file("/test_file3")
        # Then
        self.assertEqual(
            file_extractor.files, ["/test_file#test3_link", "test_file2#test_link", "/test_file3"]
        )
        self.assertEqual(
            file_extractor.hdfs_files,
            [
                "hdfs:///test_file#test3_link",
                "hdfs:///user/pig/examples/pig_test_node/test_file2#test_link",
                "hdfs:///test_file3",
            ],
        )

    def test_add_file_extra_hash(self):
        # Given
        file_extractor = FileExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        with self.assertRaises(Exception) as context:
            file_extractor.add_file("/test_file#4rarear#")
        # Then
        self.assertEqual(
            "There should be maximum one '#' in the path /test_file#4rarear#", str(context.exception)
        )

    def test_replace_el(self):
        # Given
        params = {"var1": "value1", "var2": "value2", **self.default_params}
        # language=XML
        node_str = """
<pig>
    <file>/path/with/el/${var1}</file>
    <file>/path/with/el/${var2}</file>
    <file>/path/with/two/els/${var1}/${var2}</file>
</pig>
        """
        oozie_node = ET.fromstring(node_str)
        file_extractor = FileExtractor(oozie_node=oozie_node, params=params)
        # When
        file_extractor.parse_node()
        # Then
        self.assertEqual(
            file_extractor.hdfs_files,
            [
                "hdfs:///path/with/el/value1",
                "hdfs:///path/with/el/value2",
                "hdfs:///path/with/two/els/value1/value2",
            ],
        )
