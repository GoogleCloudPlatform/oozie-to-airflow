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
"""Utils for manipulating dictionaries."""
from copy import deepcopy
from typing import Dict, Any, Tuple


def remove_key_from_dictionary_copy(dictionary: Dict, key: Any, default_value=None) -> Tuple[Dict, Any]:
    """
    Removes key from dictionary without mutating the original dictionary. Returns the tuple -
    copy of the dictionary with removed key and the value removed from the dictionary.
    In case the key is not in the dictionary returns tuple - original dictionary, default value
    :param dictionary: dictionary to remove key from
    :param key: key to remove
    :param default_value: default value to return if key not found
    :return:
    """
    if key not in dictionary:
        return dictionary, default_value
    value = dictionary[key]
    dictionary_copy = deepcopy(dictionary)
    del dictionary_copy[key]
    return dictionary_copy, value
