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
from typing import Dict, List, Optional, Union, Any, Callable, Tuple
from urllib.parse import urlparse, ParseResult

from converter.exceptions import ParseException
from o2a_libs import el_basic_functions
from o2a_libs import el_wf_functions

FN_MATCH = re.compile(r"\${\s*(\w+)\(([\w\s,\'\"\-]*)\)\s*\}")
WF_FN_MATCH = re.compile(r"\${\s*(wf:\w+)\((.*)\)\s*\}")
VAR_MATCH = re.compile(r"\${\s*([\w.]+)\s*}")

EL_CONSTANTS = {"KB": 1024 ** 1, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4, "PB": 1024 ** 5}

EL_FUNCTIONS: Dict[str, Optional[Callable]] = {
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

WF_EL_FUNCTIONS: Dict[str, Optional[Callable]] = {
    "wf:id": None,
    "wf:name": None,
    "wf:appPath": None,
    "wf:conf": el_wf_functions.wf_conf,
    "wf:user": None,
    "wf:group": None,
    "wf:callback": None,
    "wf:transition": None,
    "wf:lastErrorNode": None,
    "wf:errorCode": None,
    "wf:errorMessage": None,
    "wf:run": None,
}


def escape_string_with_python_escapes(string_to_escape: str) -> Optional[str]:
    escaped_bytes, _ = codecs.escape_encode(string_to_escape.encode())  # type: ignore # C-Api level
    return escaped_bytes.decode("utf-8")  # type: ignore


# pylint: disable=unused-argument
# noinspection PyUnusedLocal
def value_of_first_variable(
    el_string: str, properties: Dict[str, str], func_map: Dict[str, Callable] = None
) -> Optional[str]:
    """
    Finds first variable like ${variableName} and returns
    replacement string for the variable as string or None if no
    variable is found.

    :param el_string: string to look the function within
    :param properties: dictionary of job properties
    :param func_map: map of available functions
    :return replacement for the first function found or None if no variable found in string
    :raise KeyError in case variable is not found in properties
    """
    var_match = re.findall(VAR_MATCH, el_string)
    if not var_match:
        return None
    variable = var_match[0]
    if variable not in properties.keys():
        raise KeyError(f"The property {variable} cannot be found in {properties}")
    return properties[variable]


# pylint: disable=unused-argument
# noinspection PyUnusedLocal
def fstring_for_first_variable(
    el_string: str, properties: Dict[str, str], func_map: Dict[str, Callable] = None
) -> Optional[str]:
    """
    Finds first variable like ${variableName} and returns
    replacement string for the variable in the form of '{PARAMS['variableName]}'
    or None if no variable is found.

    :param el_string - string to look the variable within
    :param func_map - map of available wf functions
    :param properties - optional properties available

    :return replacement for the first function found or None if no variable found
    :raise KeyError in case property is not found
    """
    var_match = re.findall(VAR_MATCH, el_string)
    if not var_match:
        return None
    variable = var_match[0]
    if properties is not None and variable not in properties.keys():
        raise KeyError(f"The property {variable} cannot be found in {properties}")

    return f'{{CTX["{variable}"]}}'


def fstring_for_first_matched_function(
    el_string: str, match: Any, func_map: Dict[str, Optional[Callable]], is_wf_function: bool
) -> Optional[str]:
    """
    Finds replacement for the first matched function in el_string with the f-string
    corresponding to that function call.

    :param el_string: el_string to replace function with
    :param properties: currently defined properties
    :param match: regexp matching the function
    :param func_map: map of the functions (name -> function)
    :param is_wf_function: whether the function is wf_function (add CTX as first argument of
                           the function and uses different package
    :return:
    """
    fn_match = re.findall(match, el_string)
    if not fn_match:
        return None
    if fn_match[0][0] not in func_map:
        raise KeyError("EL function not supported: {}".format(fn_match[0][0]))
    mapped_func = func_map[fn_match[0][0]]
    if mapped_func is None:
        return f"NOT_IMPLEMENTED_fn_match_{fn_match[0][0]}_{fn_match[0][1]}"

    func_name = mapped_func.__name__

    # TODO: we have to later parse the whole expression language (maybe) and
    #       make sure we also handle escaping, but for now we are simply replacing
    #       the single quotes with double ones so that they need not to be escaped
    #       in the generated f-string if they appear in the arguments
    arguments = fn_match[0][1].replace("'", '"')
    if is_wf_function:
        prefix = "CTX, " if arguments else "CTX"
        return escape_string_with_python_escapes(f"{{el_wf_functions.{func_name}({prefix}{arguments})}}")
    return f"{{el_basic_functions.{func_name}({arguments})}}"


def fstring_for_first_el_function(el_string: str, properties: Dict[str, str], func_map=None):
    """
    Finds first el_function like ${ function(arg1, arg2 } and returns
    replacement string for the function in the form of '{function(arg1,arg2)}'
    or None if no function is found

    In case of functions which are not yet implemented returns
    NOT_IMPLEMENTED_function_arg1_arg2 string.


    :param el_string: string to look the function within
    :param properties: available properties
    :param func_map: map of available functions

    :return replacement for the first function found or None if no function found
    :raise KeyError in case function is found but no mapping found for the function
    """

    return fstring_for_first_matched_function(
        el_string=el_string,
        match=FN_MATCH,
        func_map=func_map if func_map is not None else EL_FUNCTIONS,
        is_wf_function=False,
    )


# pylint: disable=unused-argument
# noinspection PyUnusedLocal
def fstring_for_first_wf_el_function(
    el_string: str, properties: Dict[str, str], func_map: Dict[str, Optional[Callable]] = None
):
    """
    Finds first wf_el_function like ${ wf:function(arg1, arg2 } and returns
    replacement string for the function in the form of '{wf_function(arg1,arg2)}'
    or None if no function is found.

    In case of functions which are not yet implemented returns
    NOT_IMPLEMENTED_function_arg1_arg2 string.

    :param el_string: string to look the function within
    :param properties: available properties
    :param func_map: map of available wf functions

    :return replacement for the first function found or None if no function found
    :raise KeyError in case function is found but no mapping found for the function
    """

    return fstring_for_first_matched_function(
        el_string=el_string,
        match=WF_FN_MATCH,
        func_map=func_map if func_map is not None else WF_EL_FUNCTIONS,
        is_wf_function=True,
    )


def _replace_all_matches_with_fstrings(
    el_string: str, match_regex: Any, replace_function: Callable, properties: Dict[str, str]
):
    match = re.search(match_regex, el_string)
    while match:
        replaced_value = replace_function(el_string, properties=properties)
        el_string = re.sub(match_regex, repl=replaced_value, string=el_string, count=1)
        match = re.search(match_regex, el_string)
    return el_string


def _split_and_inline_property_string(property_string: str, properties: Dict[str, str]) -> Tuple[str, str]:
    """
    Converts a property string of form key=value to key, value tuple.
    It inlines property references within the value field with actual property values (not f-strings)

    :param property_string: property string to convert in key=value form
    :param properties: map of properties used to inline property values
    :return: key, value tuple
    """
    key, value = property_string.split("=", 1)
    value = replace_el_with_property_values(value.strip(), properties=properties)
    return key.strip(), value


# noinspection PyUnusedLocal
def replace_el_with_property_values(el_string: str, properties: Dict[str, str], python_escaped: bool = True):
    """
    Converts an EL with properties only to f-string form where all the
    property references are inlined with corresponding values.

    :param el_string: el-string to replace all properties within
    :param properties: current map of properties defined for the job
    :param python_escaped: whether returned string should be python-escaped

    :return: f-string with all properties inlined
    """
    inlined_string = _replace_all_matches_with_fstrings(
        el_string=el_string,
        match_regex=VAR_MATCH,
        replace_function=value_of_first_variable,
        properties=properties,
    )
    if python_escaped:
        return escape_string_with_python_escapes(inlined_string)
    return inlined_string


def convert_el_string_to_fstring(el_string: str, properties: Dict[str, str], python_escaped: bool = True):
    """
    Converts an EL with functions or variables to f-string form where all the
    function and property references are replaced with corresponding method calls and references.

    :param el_string: el-string to replace all functions within
    :param properties: current map of properties defined for the job
    :param python_escaped: whether returned string should be python-escaped

    :return: f-string with all el-constructs replaced
    """
    new_el_string = el_string

    new_el_string = _replace_all_matches_with_fstrings(
        el_string=new_el_string,
        match_regex=FN_MATCH,
        replace_function=fstring_for_first_el_function,
        properties=properties,
    )

    new_el_string = _replace_all_matches_with_fstrings(
        el_string=new_el_string,
        match_regex=WF_FN_MATCH,
        replace_function=fstring_for_first_wf_el_function,
        properties=properties,
    )

    new_el_string = _replace_all_matches_with_fstrings(
        el_string=new_el_string,
        match_regex=VAR_MATCH,
        replace_function=fstring_for_first_variable,
        properties=properties,
    )
    if python_escaped:
        return escape_string_with_python_escapes(new_el_string)
    return new_el_string


def parse_el_property_file_into_dictionary(properties_file: Optional[str], prop_dict: Dict[str, str] = None):
    """
    Parses the properties file into a dictionary, if the value has
    an EL variable in it, it gets inlined with the corresponding
    value that has already been parsed. For example, a file like:

    job.properties
        host=user@google.com
        command=ssh ${host}

    The properties would be parsed like:

    CTX = {
        host: 'user@google.com',
        command='ssh user@google.com',
    }
    """
    if prop_dict is None:
        prop_dict = {}
    if properties_file:
        if os.path.isfile(properties_file):
            with open(properties_file) as prop_file:
                for line in prop_file.readlines():
                    if line.startswith("#") or line.strip() == "" or line.startswith("\n"):
                        continue
                    else:
                        key, value = _split_and_inline_property_string(line, prop_dict)
                        prop_dict[key] = value
        else:
            logging.warning(f"The properties file is missing: {properties_file}")
    return prop_dict


def comma_separated_string_to_list(line: str) -> Union[List[str], str]:
    """
    Converts a comma-separated string to a List of strings.
    If the input is a single item (no comma), it will be returned unchanged.
    """
    values = line.split(",")
    return values[0] if len(values) <= 1 else values


def normalize_path_by_adding_hdfs_if_needed(
    url: str, properties: Dict[str, str], allow_no_schema: bool = False
) -> str:
    """
    Inlines all the property values in the path and normalizes it to contain hdfs:// prefix if not there
    :param url: url of the path
    :param properties: properties of the job
    :param allow_no_schema: whether to allow no-schema urls
    :return: the normalized path as string
    """
    replaced_properties = replace_el_with_property_values(url, properties=properties, python_escaped=False)
    url_with_properties_and_functions = convert_el_string_to_fstring(
        replaced_properties, properties=properties
    )
    url_parts: ParseResult = urlparse(url_with_properties_and_functions)
    allowed_schema = {"hdfs", ""} if allow_no_schema else {"hdfs"}
    if url_parts.scheme not in allowed_schema:
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_properties_and_functions}"
        )
    return url_parts.path
