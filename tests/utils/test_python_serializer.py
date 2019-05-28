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
"""Tests Python serializer"""

import unittest

from o2a.utils import python_serializer


class PythonSerializerModule(unittest.TestCase):
    """Tests Python serializer"""

    def test_should_serialize_none(self):
        self.assertEqual("None", python_serializer.serialize(None))

    def test_should_serialize_str(self):
        self.assertEqual("'DAG_NAME_A'", python_serializer.serialize("DAG_NAME_A"))

    def test_should_serialize_true(self):
        self.assertEqual("True", python_serializer.serialize(True))

    def test_should_serialize_false(self):
        self.assertEqual("False", python_serializer.serialize(False))

    def test_should_serialize_empty_list(self):
        self.assertEqual("[]", python_serializer.serialize([]))

    def test_should_serialize_complex_list(self):
        self.assertEqual(
            "['A', True, False, None, {}, {'A': 'B'}]",
            python_serializer.serialize(["A", True, False, None, {}, {"A": "B"}]),
        )

    def test_should_serialize_list_of_string(self):
        self.assertEqual("['A', 'B', 'C']", python_serializer.serialize(["A", "B", "C"]))

    def test_should_serialize_empty_dict(self):
        self.assertEqual("{}", python_serializer.serialize(dict()))

    def test_should_serialize_simple_dict(self):
        self.assertEqual("{'A': 'B', 'C': 'D'}", python_serializer.serialize({"A": "B", "C": "D"}))

    def test_should_serialize_complex_dict(self):
        self.assertEqual(
            "{'A': 'A', 'B': True, 'C': False, 'D': None, 'E': {}, 'F': {'A': 'B'}}",
            python_serializer.serialize(
                {"A": "A", "B": True, "C": False, "D": None, "E": dict(), "F": {"A": "B"}}
            ),
        )

    def test_should_serialize_empty_set(self):
        self.assertEqual("set()", python_serializer.serialize(set()))

    def test_should_serialize_simple_set(self):
        self.assertEqual("{'A'}", python_serializer.serialize({"A"}))

    def test_should_serialize_complex_set(self):
        # Set causes that the result is not deterministic
        text = python_serializer.serialize({"A", True, False, None})
        self.assertIn("{", text)
        self.assertIn("False", text)
        self.assertIn("True", text)
        self.assertIn("'A'", text)
        self.assertIn("None", text)
        self.assertIn("}", text)

    def test_should_serialize_empty_tuple(self):
        self.assertEqual("()", python_serializer.serialize(()))

    def test_should_serialize_simple_tuple(self):
        self.assertEqual("('A', 'B')", python_serializer.serialize(("A", "B")))

    def test_should_serialize_complex_tuple(self):
        self.assertEqual("('A', True, False, None)", python_serializer.serialize(("A", True, False, None)))

    def test_should_detect_circular_imports(self):
        first = ["A"]
        second = ["B", first]
        first.append(second)
        with self.assertRaisesRegex(Exception, "Circular reference detected"):
            python_serializer.serialize(first)

    def test_should_raise_exception_on_invalid_type(self):
        with self.assertRaisesRegex(ValueError, "Type '<class 'type'>' is not serializable"):
            python_serializer.serialize([unittest.TestCase])
