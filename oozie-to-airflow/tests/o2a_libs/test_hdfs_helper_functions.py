# Copyright 2018 Google LLC
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
import subprocess
import unittest
from unittest import mock

from o2a_libs import hdfs_helper_functions


class TestHelperFunctions(unittest.TestCase):

    def test_read_hdfs_files_empty(self):
        self.assertEqual([], hdfs_helper_functions.read_hdfs_files([]))

    @mock.patch('subprocess.check_output')
    def test_read_hdfs_files(self, check_mock):
        OUTPUT = 'data'
        check_mock.return_value = OUTPUT.encode('utf-8')

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            for f in hdfs_helper_functions.read_hdfs_files(['test.txt']):
                self.assertEqual('data', f.read())
                check_mock.assert_called_once_with(['hadoop', 'fs', '-cat', 'test.txt'])

    @mock.patch('subprocess.check_output')
    def test_read_hdfs_files_multiple(self, check_mock):
        OUTPUT = 'data'
        check_mock.return_value = OUTPUT.encode('utf-8')

        with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
            for f in hdfs_helper_functions.read_hdfs_files(['test.txt', 'test2.txt']):
                self.assertEqual('data', f.read())
                check_mock.assert_any_call(['hadoop', 'fs', '-cat', 'test.txt'])
                check_mock.assert_any_call(['hadoop', 'fs', '-cat', 'test2.txt'])

    @mock.patch('subprocess.check_output')
    def test_read_hdfs_file(self, check_mock):
        FN = 'test_file.txt'
        OUTPUT = 'test output'

        check_mock.return_value = OUTPUT.encode('utf-8')

        actual = hdfs_helper_functions.read_hdfs_file(FN)

        check_mock.assert_called_once_with(['hadoop', 'fs', '-cat', FN])
        self.assertEqual(OUTPUT, actual.read())

    @mock.patch('logging.WARNING')
    @mock.patch('subprocess.check_output')
    def test_read_hdfs_file_error(self, check_mock, log_mock):
        FN = 'test_file.txt'
        OUTPUT = 'test output'

        check_mock.side_effect = subprocess.CalledProcessError(1, 'hadoop')
        check_mock.return_value = OUTPUT.encode('utf-8')

        actual = hdfs_helper_functions.read_hdfs_file(FN)

        self.assertEqual(actual, None)
        log_mock.assert_called_once()
