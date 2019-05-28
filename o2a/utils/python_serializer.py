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
"""Python serializer

Serialize object to Python code.

The following data types are supported:
- str,
- dict,
- list,
- set,
- tuple,
- boolean values,
- None value;

If you encounter a circular reference, an ValueError will be thrown.
"""
from typing import Any, Set

from o2a.utils.el_utils import escape_string_with_python_escapes


def serialize(serializable_obj: Any) -> str:
    """
    Serialize to Python code
    """

    def serialize_recursively(target: Any, markers: Set[int]) -> str:
        marker_id = id(target)
        if marker_id in markers:
            raise ValueError("Circular reference detected")
        markers.add(marker_id)

        if isinstance(target, str):
            buf = f"{escape_string_with_python_escapes(target)}"
        elif isinstance(target, dict):
            buf = "{"
            buf += ", ".join(
                f"{serialize_recursively(key, markers)}: {serialize_recursively(value, markers)}"
                for key, value in target.items()
            )
            buf += "}"
        elif isinstance(target, list):
            buf = "["
            buf += ", ".join(serialize_recursively(item, markers) for item in target)
            buf += "]"
        elif isinstance(target, set):
            if target:
                buf = "{"
                buf += ", ".join(serialize_recursively(item, markers) for item in target)
                buf += "}"
            else:
                buf = "set()"
        elif isinstance(target, tuple):
            buf = "("
            buf += ", ".join(serialize_recursively(item, markers) for item in target)
            buf += ")"
        elif target is True:
            buf = "True"
        elif target is False:
            buf = "False"
        elif target is None:
            buf = "None"
        else:
            raise ValueError(f"Type '{type(target)}' is not serializable")

        markers.remove(marker_id)
        return buf

    return serialize_recursively(serializable_obj, set())
