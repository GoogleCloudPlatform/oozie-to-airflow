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

from unittest.mock import patch

from airflow import AirflowException

from o2a.o2a_libs import el_fs_functions as fs

DELIMITER = "c822c1b63853ed273b89687ac505f9fa"


class TestFsFunctions(unittest.TestCase):
    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_pig_job_executor_success(self, mock_pipe):
        output = DELIMITER + "output" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))
        mock_pipe.return_value.poll.return_value = 0

        self.assertEqual("output", fs._pig_job_executor("success"))  # pylint:disable=protected-access

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_pig_job_executor_fail(self, mock_pipe):
        output = DELIMITER + "output" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))
        mock_pipe.return_value.poll.return_value = 1

        with self.assertRaises(AirflowException):
            fs._pig_job_executor("fail")  # pylint:disable=protected-access

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_exists(self, mock_pipe):
        output = DELIMITER + "output" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))

        mock_pipe.return_value.poll.return_value = 0
        self.assertTrue(fs.exists("path/to/file"))

        mock_pipe.return_value.poll.return_value = 1
        self.assertFalse(fs.exists("path/to/file"))

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_is_dir(self, mock_pipe):
        output = DELIMITER + "output" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))

        mock_pipe.return_value.poll.return_value = 0
        self.assertTrue(fs.is_dir("path/to/file"))

        mock_pipe.return_value.poll.return_value = 1
        self.assertFalse(fs.is_dir("path/to/file"))

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_dir_size(self, mock_pipe):
        output = DELIMITER + "12" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))

        mock_pipe.return_value.poll.return_value = 0
        self.assertEqual(12, fs.dir_size("path/to/file"))

        mock_pipe.return_value.poll.return_value = 1
        self.assertEqual(-1, fs.dir_size("path/to/file"))

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_file_size(self, mock_pipe):
        output = DELIMITER + "12" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))

        mock_pipe.return_value.poll.return_value = 0
        self.assertEqual(12, fs.file_size("path/to/file"))

        mock_pipe.return_value.poll.return_value = 1
        self.assertEqual(-1, fs.file_size("path/to/file"))

    @patch("o2a.o2a_libs.el_fs_functions.subprocess.Popen")
    def test_block_size(self, mock_pipe):
        output = DELIMITER + "12" + DELIMITER
        mock_pipe.return_value.communicate.return_value = (b"", bytes(output, encoding="utf-8"))

        mock_pipe.return_value.poll.return_value = 0
        self.assertEqual(12, fs.block_size("path/to/file"))

        mock_pipe.return_value.poll.return_value = 1
        self.assertEqual(-1, fs.block_size("path/to/file"))
