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
Tests for git action shell script.
"""

from os import path

from parameterized import parameterized

from tests.script_tests.utils import ShellScriptTestCase, mock_app

GIT_SH_FILE = path.abspath(
    path.join(path.dirname(__file__), path.pardir, path.pardir, "o2a", "scripts", "git.sh")
)


class GitTestCase(ShellScriptTestCase):
    def test_success_execution(self):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(
                f"{GIT_SH_FILE} --git-uri GIT_URI --destination-path DEST_URI --region REGION "
                "--cluster CLUSTER"
            )

        self.assertEqual(0, return_code)

        list_of_command = self.get_command_calls()

        self.assertTrue(
            all(
                command.startswith(
                    "gcloud dataproc jobs submit pig --cluster=CLUSTER --region=REGION --execute"
                )
                for command in list_of_command
            )
        )

        self.assertIn("sh", list_of_command[0])
        self.assertIn("git", list_of_command[0])
        self.assertIn("clone", list_of_command[0])
        self.assertIn("GIT_URI", list_of_command[0])
        self.assertIn("master", list_of_command[0])

        self.assertIn("fs\\ -copyFromLocal", list_of_command[1])
        self.assertIn("DEST_URI", list_of_command[1])

        self.assertIn("rm", list_of_command[2])

    def test_success_execution_with_ssh_key(self):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(
                f"{GIT_SH_FILE} --git-uri GIT_URI --destination-path DEST_URI --region REGION "
                "--cluster CLUSTER --key-path KEY_PATH"
            )

        self.assertEqual(0, return_code)

        list_of_command = self.get_command_calls()

        self.assertTrue(
            all(
                command.startswith(
                    "gcloud dataproc jobs submit pig --cluster=CLUSTER --region=REGION --execute"
                )
                for command in list_of_command
            )
        )

        self.assertEqual(5, len(list_of_command))

        self.assertIn("fs\\ -copyToLocal", list_of_command[0])

        self.assertIn("sh", list_of_command[1])
        self.assertIn("git", list_of_command[1])
        self.assertIn("clone", list_of_command[1])
        self.assertIn("GIT_URI", list_of_command[1])
        self.assertIn("master", list_of_command[1])

        self.assertIn("sh", list_of_command[2])
        self.assertIn("rm", list_of_command[2])

        self.assertIn("fs\\ -copyFromLocal", list_of_command[3])

        self.assertIn("rm", list_of_command[4])

    def test_success_execution_with_custom_branch(self):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(
                f"bash {GIT_SH_FILE} --git-uri GIT_URI --destination-path DEST_URI --region REGION "
                "--cluster CLUSTER --branch development"
            )

        self.assertEqual(0, return_code)

        list_of_command = self.get_command_calls()

        self.assertEqual(3, len(list_of_command))

        self.assertIn("development", list_of_command[0])

    @parameterized.expand(
        [
            f"bash {GIT_SH_FILE} --destination-path DEST_URI --region REGION --cluster CLUSTER",
            f"bash {GIT_SH_FILE} --git-uri GIT_URI --region REGION --cluster CLUSTER",
            f"bash {GIT_SH_FILE} --git-uri GIT_URI --destination-path DEST_URI --cluster CLUSTER",
            f"bash {GIT_SH_FILE} --git-uri GIT_URI --destination-path DEST_URI --region REGION ",
        ]
    )
    def test_required_parameters(self, command):
        with mock_app("gcloud"):
            return_code = self.run_bash_command(command)

        self.assertEqual(1, return_code)
