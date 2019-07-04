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
""""
Tests the external applications. Replaces the "gcloud" external app with a "mock" script.
This script saves all its calls along with script name and arguments to the log file specified by the
environment variable "COMMAND_EXECUTION_LOG".
This allows checking the validity of the external program call by analyzing the log file.
"""

import shutil
import tempfile
import unittest
import subprocess
from os import environ, symlink, path, remove


MOCK_APP_PATH = path.abspath(path.join(path.dirname(__file__), "mock"))


# pylint: disable=invalid-name
class mock_app:
    """
    The context manager allows you to replace the app with the "mock" script.

    Creates a new temporary directory with a symbolic link to the "mock" script. The link has a name
    corresponding to the name of the mocked app. Then this directory is added to the beginning of the
    "PATH" environment variable. Now our script takes precedence over the original app.

    After the context manager is done, the environment variable "PATH" is restored to the original state.
    """

    def __init__(self, command):
        self.app_mock_dir = tempfile.mkdtemp(prefix="app-mock")
        self.old_path = environ["PATH"]
        symlink(MOCK_APP_PATH, path.join(self.app_mock_dir, command))
        environ["PATH"] = f"{self.app_mock_dir}:{self.old_path}"

    def __enter__(self):
        return self.app_mock_dir

    # Unused parameters are required according to
    # https://docs.python.org/2/reference/datamodel.html#with-statement-context-managers
    # See also:
    # https://docs.quantifiedcode.com/python-anti-patterns/correctness/exit_must_accept_three_arguments.html
    def __exit__(self, exception_type, exception_value, traceback):
        shutil.rmtree(self.app_mock_dir)
        environ["PATH"] = self.old_path
        return True


class ShellScriptTestCase(unittest.TestCase):
    """
    Prepares the environment for script tests.

    It also provides additional methods that make it easier to write tests.
    """

    def setUp(self):
        super().setUpClass()
        self.log_file = tempfile.mktemp(prefix="app-execution-log")
        environ["COMMAND_EXECUTION_LOG"] = self.log_file

    def tearDown(self):
        super().tearDown()
        if path.isfile(self.log_file):
            remove(self.log_file)
        del environ["COMMAND_EXECUTION_LOG"]

    def get_command_calls(self):
        command_calls = []
        if path.isfile(self.log_file):
            with open(self.log_file) as file:
                # Delete the trailing the new line
                file_content = file.read()[:-1]
                command_calls = file_content.split("\n")
        return command_calls

    @staticmethod
    def run_bash_command(command):
        process = subprocess.run(
            args=["bash", "-c", command], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        return process.returncode
