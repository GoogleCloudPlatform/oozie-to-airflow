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
Tests for prepare action shell script
"""

from os import path
from parameterized import parameterized

from tests.script_tests.utils import ShellScriptTestCase, mock_app

PREPARE_SH_FILE = path.abspath(
    path.join(path.dirname(__file__), path.pardir, path.pardir, "o2a", "scripts", "prepare.sh")
)


class PrepareTestCase(ShellScriptTestCase):
    def _assert_make(self, command, dir_name):
        self.assertIn("fs", command)
        self.assertIn("-mkdir", command)
        self.assertIn("-p", command)
        self.assertIn(dir_name, command)

    def _assert_delete(self, command, dir_name):
        self.assertIn("fs", command)
        self.assertIn("-rm", command)
        self.assertIn("-f", command)
        self.assertIn("-r", command)
        self.assertIn(dir_name, command)

    def test_success_execution(self):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(
                f"{PREPARE_SH_FILE} -r REGION -c CLUSTER -d DEL_DIR -m MK_DIR"
            )

        self.assertEqual(0, return_code)

        list_of_command = self.get_command_calls()

        self.assertEqual(len(list_of_command), 2)

        self.assertTrue(
            all(
                command.startswith(
                    "gcloud dataproc jobs submit pig --cluster=CLUSTER --region=REGION --execute"
                )
                for command in list_of_command
            )
        )

        self._assert_delete(list_of_command[0], "DEL_DIR")
        self._assert_make(list_of_command[1], "MK_DIR")

    def test_success_with_many_params(self):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(
                f"{PREPARE_SH_FILE} -r REGION -c CLUSTER -d DEL_DIR1 -d DEL_DIR2 -m MK_DIR1 -m MK_DIR2"
            )

        self.assertEqual(0, return_code)

        list_of_command = self.get_command_calls()

        self.assertEqual(len(list_of_command), 4)

        self.assertTrue(
            all(
                command.startswith(
                    "gcloud dataproc jobs submit pig --cluster=CLUSTER --region=REGION --execute"
                )
                for command in list_of_command
            )
        )

        self._assert_delete(list_of_command[0], "DEL_DIR1")
        self._assert_delete(list_of_command[1], "DEL_DIR2")
        self._assert_make(list_of_command[2], "MK_DIR1")
        self._assert_make(list_of_command[3], "MK_DIR2")

    @parameterized.expand(
        [
            f"{PREPARE_SH_FILE} -r ",
            f"{PREPARE_SH_FILE} -r REGION -c",
            f"{PREPARE_SH_FILE} -r REGION -c CLUSTER -d ",
            f"{PREPARE_SH_FILE} -r REGION -c CLUSTER -d DEL_DIR -m",
            f"{PREPARE_SH_FILE} -r REGION -c CLUSTER -f",
        ]
    )
    def test_required_parameters(self, command):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(command)

        self.assertEqual(1, return_code)
