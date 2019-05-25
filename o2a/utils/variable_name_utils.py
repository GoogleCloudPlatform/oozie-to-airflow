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
"""Variable name utilities"""
import re


def convert_to_python_variable(name: str) -> str:
    """
    Creates an identifier that is a valid variable name in Python language.
    """
    # Replace all hyphens with underscores
    name = name.replace("-", "_")
    # Replace invalid characters to underscore
    name = re.sub("[^0-9a-zA-Z_]", "_", name)
    # Remove leading characters until we find a letter or underscore
    name = re.sub("^[^a-zA-Z_]+", "", name)
    return name
