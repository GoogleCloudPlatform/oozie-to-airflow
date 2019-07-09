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

"""Utilities to deal with files in workflow folders"""
import os
from typing import List


def get_lib_files(library_folder_path: str, extension: str) -> List[str]:
    """Returns list of all lib files from the library folder matching the extension provided.
    For example if you specify the '.jar' extension it will return names (not paths) of all
    the *.jar files.

    It returns an empty list in case there are no files matching the extension and raises Exception
    in case the lib folder does not exist or is not a directory.
     """
    if os.path.exists(library_folder_path):
        if os.path.isdir(library_folder_path):
            return [file for file in os.listdir(library_folder_path) if file.endswith(extension)]
        raise Exception(f"The {library_folder_path} exists but it is not a directory!")
    return []
