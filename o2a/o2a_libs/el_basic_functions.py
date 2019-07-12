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
"""Basic EL functions of the Oozie workflow"""


def first_not_null(str_one, str_two):
    """
    It returns the first not null value, or null if both are null.

    Note that if the output of this function is null and it is used as string,
    the EL library converts it to an empty string. This is the common behavior
    when using firstNotNull() in node configuration sections.
    """
    if str_one:
        return str_one
    return str_two if str_two else ""
