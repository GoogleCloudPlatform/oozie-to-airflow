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
from urllib.parse import urlparse, ParseResult

from jinja2 import StrictUndefined, Environment
from jinja2.exceptions import UndefinedError

from o2a.converter.exceptions import ParseException
from o2a.o2a_libs import el_basic_functions, el_parser
from o2a.o2a_libs.property_utils import PropertySet

FN_MATCH = re.compile(r"\${\s?(\w+)\(([\w\s,\'\"\-]*)\)\s?\}")
VAR_MATCH = re.compile(r"\${([\w.]+)}")

EL_CONSTANTS = {"KB": 1024 ** 1, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4, "PB": 1024 ** 5}

EL_FUNCTIONS = {
    "firstNotNull": el_basic_functions.first_not_null,
    "concat": el_basic_functions.concat,
    "replaceAll": el_basic_functions.replace_all,
    "appendAll": el_basic_functions.append_all,
    "trim": el_basic_functions.trim,
    "urlEncode": el_basic_functions.url_encode,
    "timestamp": el_basic_functions.timestamp,
    "toJsonStr": el_basic_functions.to_json_str,
    "toPropertiesStr": None,
    "toConfigurationStr": None,
}

WF_EL_FUNCTIONS = {
    "wf:id": None,
    "wf:name": None,
    "wf:appPath": None,
    "wf:conf": None,
    "wf:user": None,
    "wf:group": None,
    "wf:callback": None,
    "wf:transition": None,
    "wf:lastErrorNode": None,
    "wf:errorCode": None,
    "wf:errorMessage": None,
    "wf:run": None,
}


def strip_el(el_function: str) -> str:
    """
    Given an el function or variable like ${ variable },
    strips out everything except for the variable.
    """

    return re.sub("[${}]", "", el_function).strip()


def replace_el_with_var(el_function: str, props: PropertySet, quote=True) -> str:
    """
    Only supports a single variable for now.
    """
    # Matches oozie EL variables e.g. ${hostname}
    var_match = VAR_MATCH.findall(el_function)

    jinjafied_el = el_function
    if var_match:
        for var in var_match:
            try:
                value = props.merged[var]
                jinjafied_el = jinjafied_el.replace("${" + var + "}", value)
            except KeyError:
                logging.info(f"The EL variable {var} was missing in the properties")

    return "'" + jinjafied_el + "'" if quote else jinjafied_el


def parse_el_func(el_function, el_func_map=None):
    # Finds things like ${ function(arg1, arg2 } and returns
    # a list like ['function', 'arg1, arg2']
    if el_func_map is None:
        el_func_map = EL_FUNCTIONS
    fn_match = FN_MATCH.findall(el_function)

    if not fn_match:
        return None

    # fn_match is of the form [('concat', "'ls', '-l'")]
    # for an el function like ${concat('ls', '-l')}
    if fn_match[0][0] not in el_func_map:
        raise KeyError("{} EL function not supported.".format(fn_match[0][0]))

    mapped_func = el_func_map[fn_match[0][0]]

    func_name = mapped_func.__name__
    return "{}({})".format(func_name, fn_match[0][1])


def convert_el_to_jinja(oozie_el, quote=True):
    """
    Converts an EL with either a function or a variable to the form:
    Variable:
        ${variable} -> {{ job_properties.variable }}
        ${func()} -> mapped_func()

    Only supports a single variable or a single EL function.

    If quote is true, returns the string surround in single quotes, unless it
    is a function, then no quotes are added.
    """
    # Matches oozie EL functions e.g. ${concat()}
    fn_match = FN_MATCH.findall(oozie_el)
    # Matches oozie EL variables e.g. ${hostname}
    var_match = VAR_MATCH.findall(oozie_el)

    jinjafied_el = oozie_el

    if fn_match:
        jinjafied_el = parse_el_func(oozie_el)
        return jinjafied_el
    if var_match:
        for var in var_match:
            jinjafied_el = jinjafied_el.replace("${" + var + "}", "{{ params.props.merged['" + var + "'] }}")

    return "'" + jinjafied_el + "'" if quote else jinjafied_el


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


def normalize_path(url: str, props: PropertySet, allow_no_schema=False) -> str:
    """
    Replaces all EL variables in the url, validates schema and returns only the 'path' part of a url.
    Example: hdfs://localhost:8082/user/root --> user/root
    """
    url_with_var = replace_el_with_var(url, props=props, quote=False)
    url_parts: ParseResult = urlparse(url_with_var)
    if not is_allowed_schema(allow_no_schema, url_with_var):
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_var}"
        )
    return url_parts.path


def replace_url_el(url: str, props: PropertySet, allow_no_schema=False) -> str:
    """
    Replaces all EL variables in the url, validates schema and returns the url.
    """
    url_with_var = replace_el_with_var(url, props=props, quote=False)
    if not is_allowed_schema(allow_no_schema, url_with_var):
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_var}"
        )
    return url_with_var


def is_allowed_schema(allow_no_schema: bool, url_with_var: str) -> bool:
    url_parts: ParseResult = urlparse(url_with_var)
    allowed_schema = {"hdfs", ""} if allow_no_schema else {"hdfs"}
    return url_parts.scheme in allowed_schema


def escape_string_with_python_escapes(string_to_escape: Optional[str]) -> Optional[str]:
    if not string_to_escape:
        return None
    escaped_bytes, _ = codecs.escape_encode(string_to_escape.encode())  # type: ignore # C-Api level
    return "'" + escaped_bytes.decode("utf-8") + "'"  # type: ignore
