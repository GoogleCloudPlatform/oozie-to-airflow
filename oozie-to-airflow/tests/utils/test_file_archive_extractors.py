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
"""Tests Archive mapper"""
import unittest
from xml.etree import ElementTree as ET

from utils.file_archive_extractors import extract_archive_paths


class TestArchiveExtractor(unittest.TestCase):
    def setUp(self):
        self.default_params = {
            "nameNode": "hdfs://",
            "oozie.wf.application.path": "hdfs:///user/pig/examples/pig_test_node",
        }

    def test_relative_archive_normal(self):
        # Given
        node = self._get_archive_node(["test_archive.zip"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=False)
        # Then
        self.assertEqual(archives, ["test_archive.zip"])

    def test_relative_archive_hdfs(self):
        # Given
        node = self._get_archive_node(["test_archive.zip"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=True)
        # Then
        self.assertEqual(archives, ["hdfs:///user/pig/examples/pig_test_node/test_archive.zip"])

    def test_absolute_archive_normal(self):
        # Given
        node = self._get_archive_node(["test_archive.zip"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=False)
        # Then
        self.assertEqual(archives, ["test_archive.zip"])

    def test_absolute_archive_hdfs(self):
        # Given
        node = self._get_archive_node(["test_archive.zip"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=True)
        # Then
        self.assertEqual(archives, ["hdfs:///user/pig/examples/pig_test_node/test_archive.zip"])

    def test_multiple_archives_normal(self):
        # Given
        node = self._get_archive_node(["test_archive.zip", "/test_archive.zip", "/test_archive3.tar.gz"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=False)
        # Then
        self.assertEqual(archives, ["test_archive.zip", "/test_archive.zip", "/test_archive3.tar.gz"])

    def test_multiple_archives_hdfs(self):
        # Given
        node = self._get_archive_node(["test_archive.zip", "/test_archive.zip", "/test_archive3.tar.gz"])
        # When
        archives = extract_archive_paths(node, params=self.default_params, hdfs_path=True)
        # Then
        self.assertEqual(
            archives,
            [
                "hdfs:///user/pig/examples/pig_test_node/test_archive.zip",
                "hdfs:///test_archive.zip",
                "hdfs:///test_archive3.tar.gz",
            ],
        )

    def test_hash_archives_normal(self):
        # Given
        node = self._get_archive_node(
            ["/test_archive.zip#test3_link", "test_archive2.tar#test_link", "/test_archive3.tar.gz"]
        )
        # When
        archives = extract_archive_paths(node, self.default_params, hdfs_path=False)
        # Then
        self.assertEqual(
            archives, ["/test_archive.zip#test3_link", "test_archive2.tar#test_link", "/test_archive3.tar.gz"]
        )

    def test_hash_archives_hdfs(self):
        # Given
        node = self._get_archive_node(
            ["/test_archive.zip#test3_link", "test_archive2.tar#test_link", "/test_archive3.tar.gz"]
        )
        # When
        archives = extract_archive_paths(node, self.default_params, hdfs_path=True)
        # Then
        self.assertEqual(
            archives,
            [
                "hdfs:///test_archive.zip#test3_link",
                "hdfs:///user/pig/examples/pig_test_node/test_archive2.tar#test_link",
                "hdfs:///test_archive3.tar.gz",
            ],
        )

    def test_archive_extra_hash(self):
        # Given
        node = self._get_archive_node(["/test_archive.zip#4rarear#"])
        # When
        with self.assertRaisesRegex(
            Exception, "There should be maximum one '#' in the path /test_archive.zip#4rarear#"
        ):
            extract_archive_paths(node, self.default_params, hdfs_path=False)

    def test_replace_el(self):
        # Given
        params = {"var1": "value1", "var2": "value2", **self.default_params}
        node = self._get_archive_node(
            [
                "/path/with/el/${var1}.tar",
                "/path/with/el/${var2}.tar",
                "/path/with/two/els/${var1}/${var2}.tar",
            ]
        )
        # When
        archives = extract_archive_paths(node, params, hdfs_path=True)
        # Then
        self.assertEqual(
            [
                "hdfs:///path/with/el/value1.tar",
                "hdfs:///path/with/el/value2.tar",
                "hdfs:///path/with/two/els/value1/value2.tar",
            ],
            archives,
        )

    @staticmethod
    def _get_archive_node(paths):
        root = ET.Element("pig")
        for path in paths:
            archive = ET.SubElement(root, "archive")
            archive.text = path
        return root
