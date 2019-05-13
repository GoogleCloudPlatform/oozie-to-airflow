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
"""Converts variable name to pythonic one"""
import re


def convert_to_python_variable(name: str):
    # Replace all hyphens with underscores
    name = name.replace("-", "_")
    # Remove invalid characters
    name = re.sub("[^0-9a-zA-Z_]", "", name)
    # Remove leading characters until we find a letter or underscore
    name = re.sub("^[^a-zA-Z_]+", "", name)
    return name
