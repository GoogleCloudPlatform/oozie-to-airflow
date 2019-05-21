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
"""Common definitions used across the converter"""
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
TPL_PATH = os.path.join(ROOT_DIR, "templates/")

O2A_PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
EXAMPLES_PATH = os.path.join(O2A_PROJECT_PATH, "examples")

# Mapper examples
EXAMPLE_DEMO_PATH = os.path.join(EXAMPLES_PATH, "demo")
EXAMPLE_EL_PATH = os.path.join(EXAMPLES_PATH, "el")
EXAMPLE_PIG_PATH = os.path.join(EXAMPLES_PATH, "pig")
EXAMPLE_SUBWORKFLOW_PATH = os.path.join(EXAMPLES_PATH, "subwf")
