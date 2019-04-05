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
"""Tests File mixin"""
import unittest

from mappers.file_archive_mixins import FileMixin


class TestFileMixin(unittest.TestCase):
    def setUp(self):
        self.default_params = {
            "nameNode": "hdfs://",
            "oozie.wf.application.path": "hdfs:///user/pig/examples/pig_test_node",
        }

    def test_add_relative_file(self):
        # Given
        file_mixin = FileMixin(params=self.default_params)
        # When
        file_mixin.add_file("test_file")
        # Then
        self.assertEqual(file_mixin.files, "test_file")
        self.assertEqual(file_mixin.hdfs_files, "hdfs:///user/pig/examples/pig_test_node/test_file")

    def test_add_absolute_file(self):
        # Given
        file_mixin = FileMixin(params=self.default_params)
        # When
        file_mixin.add_file("/test_file")
        # Then
        self.assertEqual(file_mixin.files, "/test_file")
        self.assertEqual(file_mixin.hdfs_files, "hdfs:///test_file")

    def test_add_multiple_files(self):
        # Given
        file_mixin = FileMixin(params=self.default_params)
        # When
        file_mixin.add_file("/test_file")
        file_mixin.add_file("test_file2")
        file_mixin.add_file("/test_file3")
        # Then
        self.assertEqual(file_mixin.files, "/test_file,test_file2,/test_file3")
        self.assertEqual(
            file_mixin.hdfs_files,
            "hdfs:///test_file," "hdfs:///user/pig/examples/pig_test_node/test_file2," "hdfs:///test_file3",
        )

    def test_add_hash_files(self):
        # Given
        file_mixin = FileMixin(params=self.default_params)
        # When
        file_mixin.add_file("/test_file#test3_link")
        file_mixin.add_file("test_file2#test_link")
        file_mixin.add_file("/test_file3")
        # Then
        self.assertEqual(file_mixin.files, "/test_file#test3_link,test_file2#test_link,/test_file3")
        self.assertEqual(
            file_mixin.hdfs_files,
            "hdfs:///test_file#test3_link,"
            "hdfs:///user/pig/examples/pig_test_node/test_file2#test_link,"
            "hdfs:///test_file3",
        )

    def test_add_file_extra_hash(self):
        # Given
        file_mixin = FileMixin(params=self.default_params)
        # When
        with self.assertRaises(Exception) as context:
            file_mixin.add_file("/test_file#4rarear#")
        # Then
        self.assertEqual(
            "There should be maximum one '#' in the path /test_file#4rarear#", str(context.exception)
        )
