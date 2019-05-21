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
from xml.etree.ElementTree import Element
from xml.etree import ElementTree as ET

from o2a.utils.file_archive_extractors import ArchiveExtractor


class TestArchiveExtractor(unittest.TestCase):
    def setUp(self):
        self.default_params = {
            "nameNode": "hdfs://",
            "oozie.wf.application.path": "hdfs:///user/pig/examples/pig_test_node",
        }

    def test_add_relative_archive(self):
        # Given
        archive_extractor = ArchiveExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        archive_extractor.add_archive("test_archive.zip")
        # Then
        self.assertEqual(archive_extractor.archives, ["test_archive.zip"])
        self.assertEqual(
            archive_extractor.hdfs_archives, ["hdfs:///user/pig/examples/pig_test_node/test_archive.zip"]
        )

    def test_add_absolute_archive(self):
        # Given
        archive_extractor = ArchiveExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        archive_extractor.add_archive("/test_archive.zip")
        # Then
        self.assertEqual(archive_extractor.archives, ["/test_archive.zip"])
        self.assertEqual(archive_extractor.hdfs_archives, ["hdfs:///test_archive.zip"])

    def test_add_multiple_archives(self):
        # Given
        archive_extractor = ArchiveExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        archive_extractor.add_archive("/test_archive.zip")
        archive_extractor.add_archive("test_archive2.tar")
        archive_extractor.add_archive("/test_archive3.tar.gz")
        # Then
        self.assertEqual(
            archive_extractor.archives, ["/test_archive.zip", "test_archive2.tar", "/test_archive3.tar.gz"]
        )
        self.assertEqual(
            archive_extractor.hdfs_archives,
            [
                "hdfs:///test_archive.zip",
                "hdfs:///user/pig/examples/pig_test_node/test_archive2.tar",
                "hdfs:///test_archive3.tar.gz",
            ],
        )

    def test_add_hash_archives(self):
        # Given
        archive_extractor = ArchiveExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        archive_extractor.add_archive("/test_archive.zip#test3_link")
        archive_extractor.add_archive("test_archive2.tar#test_link")
        archive_extractor.add_archive("/test_archive3.tar.gz")
        # Then
        self.assertEqual(
            archive_extractor.archives,
            ["/test_archive.zip#test3_link", "test_archive2.tar#test_link", "/test_archive3.tar.gz"],
        )
        self.assertEqual(
            archive_extractor.hdfs_archives,
            [
                "hdfs:///test_archive.zip#test3_link",
                "hdfs:///user/pig/examples/pig_test_node/test_archive2.tar#test_link",
                "hdfs:///test_archive3.tar.gz",
            ],
        )

    def test_add_archive_extra_hash(self):
        # Given
        archive_extractor = ArchiveExtractor(oozie_node=Element("fake"), params=self.default_params)
        # When
        with self.assertRaises(Exception) as context:
            archive_extractor.add_archive("/test_archive.zip#4rarear#")
        # Then
        self.assertEqual(
            "There should be maximum one '#' in the path /test_archive.zip#4rarear#", str(context.exception)
        )

    def test_replace_el(self):
        # Given
        params = {"var1": "value1", "var2": "value2", **self.default_params}
        # language=XML
        node_str = """
<pig>
    <archive>/path/with/el/${var1}.tar</archive>
    <archive>/path/with/el/${var2}.tar</archive>
    <archive>/path/with/two/els/${var1}/${var2}.tar</archive>
</pig>
        """
        oozie_node = ET.fromstring(node_str)
        archive_extractor = ArchiveExtractor(oozie_node=oozie_node, params=params)
        # When
        archive_extractor.parse_node()
        # Then
        self.assertEqual(
            [
                "hdfs:///path/with/el/value1.tar",
                "hdfs:///path/with/el/value2.tar",
                "hdfs:///path/with/two/els/value1/value2.tar",
            ],
            archive_extractor.hdfs_archives,
        )
