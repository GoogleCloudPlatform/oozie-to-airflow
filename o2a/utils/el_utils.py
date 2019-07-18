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
"""Utilities used by EL functions"""
import codecs
import logging
import os
import re
from copy import deepcopy
from typing import List, Optional, Tuple, Union, Dict
from urllib.parse import urlparse

from jinja2 import StrictUndefined, Environment
from jinja2.exceptions import UndefinedError

from o2a.converter.exceptions import ParseException
from o2a.o2a_libs import el_parser
from o2a.o2a_libs.property_utils import PropertySet


def strip_el(el_function: str) -> str:
    """
    Given an el function or variable like ${ variable },
    strips out everything except for the variable.
    """
    return re.sub("[${}]", "", el_function).strip()


def extract_evaluate_properties(properties_file: Optional[str], props: PropertySet):
    """
    Parses the job_properties file into a dictionary, if the value has
    and EL function in it, it gets replaced with the corresponding
    value that has already been parsed. For example, a file like:
    job.job_properties
        host=user@google.com
        command=ssh ${host}
    The job_properties would be parsed like:
        PROPERTIES = {
        host: 'user@google.com',
        command='ssh user@google.com',
    }
    """
    copy_of_props = deepcopy(props)
    properties_read_from_file: Dict[str, str] = {}
    if not properties_file:
        return properties_read_from_file

    if not os.path.isfile(properties_file):
        logging.warning(f"The job_properties file is missing: {properties_file}")
        return properties_read_from_file

    with open(properties_file) as prop_file:
        for line in prop_file.readlines():
            if line.startswith("#") or line.startswith(" ") or line.startswith("\n"):
                continue

            key, value = _evaluate_properties_line(
                line, known_values=properties_read_from_file, props=copy_of_props
            )
            # Set the value of property in the copy of property set for further reference
            copy_of_props.action_node_properties[key] = value
            properties_read_from_file[key] = value

    return properties_read_from_file


def _evaluate_properties_line(line: str, known_values: dict, props: PropertySet) -> Tuple[str, str]:
    """
    Evaluates single line from properties file using already known values from the file and
    values from passed property set.
    """
    key, value = line.split("=", 1)
    translation = el_parser.translate(value)

    tmp = deepcopy(known_values)
    tmp.update(props.merged)
    env = Environment(undefined=StrictUndefined)
    try:
        translation = env.from_string(translation).render(**tmp)
    except UndefinedError:
        translation = value

    return key.strip(), translation.strip()


def comma_separated_string_to_list(line: str) -> Union[List[str], str]:
    """
    Converts a comma-separated string to a List of strings.
    If the input is a single item (no comma), it will be returned unchanged.
    """
    values = line.split(",")
    return values[0] if len(values) <= 1 else values


def _resolve_name_node(translation: str, props: PropertySet) -> Tuple[Optional[str], int]:
    """
    Check if props include nameNode, nameNode1 or nameNode2 value.
    """
    merged = props.merged
    for key in ["nameNode", "nameNode1", "nameNode2"]:
        start_str = "{{" + key + "}}"
        name_node = merged.get(key)
        if translation.startswith(start_str) and name_node:
            return name_node, len(start_str)
    return None, 0


def normalize_path(url: str, props: PropertySet, allow_no_schema=False, translated=False) -> str:
    """
    Transforms url by replacing EL-expression with equivalent jinja templates
    and returns only the path part of the url. If schema validation is
    required then props should include proper name-node. If translated is set to True
    then passed url is supposed to be a valid jinja expression.
    For example:
        input: '{$nameNode}/users/{$userName}/dir
        url_with_var: `{{nameNode}}/users/{{userName}}/dir
    In this case to validate url schema props should contain `nameNode` value.
    """
    url_with_var = url if translated else el_parser.translate(url)

    name_node, shift = _resolve_name_node(url_with_var, props)
    if name_node:
        url_parts = urlparse(name_node)
        output = url_with_var[shift:]
    else:
        url_parts = urlparse(url_with_var)
        output = url_parts.path

    allowed_schemas = {"hdfs", ""} if allow_no_schema else {"hdfs"}
    if url_parts.scheme not in allowed_schemas:
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_var}"
        )

    return output


def replace_url_el(url: str, props: PropertySet, allow_no_schema=False) -> str:
    """
    Transforms url by replacing EL-expression with equivalent jinja templates.
    If schema validation is required then props should include proper name-node.
    For example:
        input: '{$nameNode}/users/{$userName}/dir
        url_with_var: `{{nameNode}}/users/{{userName}}/dir
    In this case to validate url schema props should contain `nameNode` value.
    """
    url_with_var = el_parser.translate(url)

    name_node, _ = _resolve_name_node(url_with_var, props)
    if name_node:
        url_parts = urlparse(name_node)
    else:
        url_parts = urlparse(url_with_var)

    allowed_schemas = {"hdfs", ""} if allow_no_schema else {"hdfs"}
    if url_parts.scheme not in allowed_schemas:
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_var}"
        )

    return url_with_var


def escape_string_with_python_escapes(string_to_escape: Optional[str]) -> Optional[str]:
    if not string_to_escape:
        return None
    escaped_bytes, _ = codecs.escape_encode(string_to_escape.encode())  # type: ignore # C-Api level
    return "'" + escaped_bytes.decode("utf-8") + "'"  # type: ignore
