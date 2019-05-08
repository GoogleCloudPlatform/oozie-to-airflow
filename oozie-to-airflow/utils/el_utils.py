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
import logging
import os
import re
from typing import Dict, List, Optional, Union, Any, Callable
from urllib.parse import urlparse, ParseResult

from converter.exceptions import ParseException
from o2a_libs import el_basic_functions
from o2a_libs import el_wf_functions

FN_MATCH = re.compile(r"\${\s*(\w+)\(([\w\s,\'\"\-]*)\)\s*\}")
WF_FN_MATCH = re.compile(r"\${\s*(wf:\w+)\(([\"\'][^\"\']*[\"\'])\)\s*\}")
VAR_MATCH = re.compile(r"\${\s*([\w.]+)\s*}")

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


# noinspection PyUnusedLocal
def _value_of_first_variable(
    el_string: str,
    func_map: Dict[str, Callable] = None,
    quote_character: str = "",
    parameters: Dict[str, str] = None,
) -> str:
    """
    Finds first variable like ${variableName} and returns
    replacement string for the variable as string or None if no
    variable is found.

    :param el_string - string to look the function within
    :param quote_character - character used for quoting (usually ', " or empty string for no quoting.
    :param func_map - map of available functions
    :param parameters - dictionary of parameters

    :return replacement for the first function found or None if no variable found
    :raise KeyError in case variable is not found in parameters
    """
    var_match = re.findall(VAR_MATCH, el_string)
    if not var_match:
        return None
    else:
        variable = var_match[0]
        if parameters and variable not in parameters.keys():
            raise KeyError(f"The parameter {variable} cannot be found in {parameters}")
        return parameters[variable]


def _code_of_first_el_function(
    el_string: str,
    func_map: Dict[str, Callable] = None,
    quote_character: str = "",
    parameters: Dict[str, str] = None,
):
    """
    Finds first el_function like ${ function(arg1, arg2 } and returns
    replacement string for the function in the form of '{function(arg1,arg2)}'
    or None if no function is found

    In case of functions which are not yet implemented returns
    NOT_IMPLEMENTED_function_arg1_arg2 string.


    :param el_string - string to look the function within
    :param quote_character - character used for quoting (usually ', " or empty string for no quoting.
    :param func_map - map of available functions
    :param parameters - dictionary of parameters

    :return replacement for the first function found or None if no function found
    :raise KeyError in case function is found but no mapping found for the function
    """
    if func_map is None:
        func_map = EL_FUNCTIONS

    return _match_function(
        el_string=el_string,
        match=FN_MATCH,
        func_map=func_map,
        add_context=False,
        quote_character=quote_character,
    )


# noinspection PyUnusedLocal
def _code_of_first_wf_el_function(
    el_string: str,
    quote_character: str,
    func_map: Dict[str, Callable] = None,
    parameters: Dict[str, str] = None,
):
    """
    Finds first wf_el_function like ${ wf:function(arg1, arg2 } and returns
    replacement string for the function in the form of '{wf_function(arg1,arg2)}'
    or None if no function is found.

    In case of functions which are not yet implemented returns
    NOT_IMPLEMENTED_function_arg1_arg2 string.

    :param el_string - string to look the function within
    :param quote_character - character used for quoting (usually ', " or empty string for no quoting.
    :param func_map - map of available wf functions
    :param parameters - optional parameters available

    :return replacement for the first function found or None if no function found
    :raise KeyError in case function is found but no mapping found for the function
    """
    # Finds things like ${wf:function(arg1, arg2 } and returns
    # a function in the form of 'wf_function(arg1,arg2)'
    if func_map is None:
        func_map = WF_EL_FUNCTIONS

    return _match_function(
        el_string=el_string,
        match=WF_FN_MATCH,
        func_map=func_map,
        add_context=True,
        quote_character=quote_character,
    )


# noinspection PyUnusedLocal
def _code_of_first_variable(
    el_string: str,
    quote_character: str,
    func_map: Dict[str, Callable] = None,
    parameters: Dict[str, str] = None,
) -> str:
    """
    Finds first variable like ${variableName} and returns
    replacement string for the variable in the form of '{PARAMS['variableName]}'
    or None if no variable is found.

    :param el_string - string to look the variable within
    :param quote_character - character used for quoting (usually ', " or empty string for no quoting.
    :param func_map - map of available wf functions
    :param parameters - optional parameters available

    :return replacement for the first function found or None if no variable found
    :raise KeyError in case parameter is not found
    """
    var_match = re.findall(VAR_MATCH, el_string)
    if not var_match:
        return None
    else:
        variable = var_match[0]
        if parameters and variable not in parameters.keys():
            raise KeyError(f"The parameter {variable} cannot be found in {parameters}")

        the_other_quote_character = _the_other_quote(quote_character)
        return f"{{DAG_CONTEXT.params[{the_other_quote_character}{variable}{the_other_quote_character}]}}"


def _replace_all_matches(
    el_string: str,
    match_regex: Any,
    replace_function: Callable,
    quote_character: str,
    parameters: Dict[str, str],
):
    match = re.search(match_regex, el_string)
    while match:
        replaced_value = replace_function(el_string, quote_character=quote_character, parameters=parameters)
        el_string = re.sub(match_regex, repl=replaced_value, string=el_string, count=1)
        match = re.search(match_regex, el_string)
    return el_string


def _the_other_quote(quote_character):
    return "'" if quote_character == '"' else '"'


# noinspection PyUnusedLocal
def _match_function(
    el_string: str, match: Any, func_map: Dict[str, Callable], add_context: bool, quote_character: str
) -> Optional[str]:
    fn_match = re.findall(match, el_string)
    if not fn_match:
        return None
    else:
        if fn_match[0][0] not in func_map:
            raise KeyError("EL function not supported: {}".format(fn_match[0][0]))
    mapped_func = func_map[fn_match[0][0]]
    if mapped_func is None:
        return f"NOT_IMPLEMENTED_fn_match_{fn_match[0][0]}_{fn_match[0][1]}"

    func_name = mapped_func.__name__

    if quote_character != "":
        arguments = fn_match[0][1].replace(quote_character, _the_other_quote(quote_character))
    else:
        arguments = fn_match[0][1]
    if add_context:
        return f"{{{func_name}(DAG_CONTEXT, {arguments})}}"
    else:
        return f"{{{func_name}({arguments})}}"


def _convert_line(line: str, prop_dict: Dict[str, str]) -> None:
    """
    Converts a line from the properties file and adds it to the properties dictionary.
    """
    key, value = line.split("=", 1)
    value = replace_el_with_var_value(value.strip(), prop_dict)
    prop_dict[key.strip()] = value


# noinspection PyUnusedLocal
def replace_el_with_var_value(el_string: str, parameters: Dict[str, str], quote_character: str = ""):
    return _replace_all_matches(
        el_string=el_string,
        match_regex=VAR_MATCH,
        replace_function=_value_of_first_variable,
        quote_character=quote_character,
        parameters=parameters,
    )


def convert_el_to_string(el_string: str, quote_character='"', parameters: Dict[str, str] = None):
    """
    Converts an EL with functions or variables to the form:

    If quote character is set, it returns the arguments are surrounded with
    the other quotes (" -> '  or ' -> ") and the whole string is surrounded
    with quote_character provided
    """
    # Matches oozie EL functions e.g. ${concat()}

    new_el_string = el_string

    new_el_string = _replace_all_matches(
        el_string=new_el_string,
        match_regex=FN_MATCH,
        replace_function=_code_of_first_el_function,
        quote_character=quote_character,
        parameters=parameters,
    )

    new_el_string = _replace_all_matches(
        el_string=new_el_string,
        match_regex=WF_FN_MATCH,
        replace_function=_code_of_first_wf_el_function,
        quote_character=quote_character,
        parameters=parameters,
    )

    new_el_string = _replace_all_matches(
        el_string=new_el_string,
        match_regex=VAR_MATCH,
        replace_function=_code_of_first_variable,
        quote_character=quote_character,
        parameters=parameters,
    )

    return f"{quote_character}{new_el_string}{quote_character}"


def parse_els(properties_file: Optional[str], prop_dict: Dict[str, str] = None):
    """
    Parses the properties file into a dictionary, if the value has
    and EL function in it, it gets replaced with the corresponding
    value that has already been parsed. For example, a file like:

    job.properties
        host=user@google.com
        command=ssh ${host}

    The params would be parsed like:

    DAG_CONTEXT.params = {
        host: 'user@google.com',
        command='ssh user@google.com',
    }
    """
    if prop_dict is None:
        prop_dict = {}
    if properties_file:
        if os.path.isfile(properties_file):
            with open(properties_file, "r") as prop_file:
                for line in prop_file.readlines():
                    if line.startswith("#") or line.startswith(" ") or line.startswith("\n"):
                        continue
                    else:
                        _convert_line(line, prop_dict)
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


def normalize_path(url, params, allow_no_schema=False):
    url_with_var = replace_el_with_var_value(url, parameters=params)
    url_parts: ParseResult = urlparse(url_with_var)
    allowed_schema = {"hdfs", ""} if allow_no_schema else {"hdfs"}
    if url_parts.scheme not in allowed_schema:
        raise ParseException(
            f"Unknown path format. The URL should be provided in the following format: "
            f"hdfs://localhost:9200/path. Current value: {url_with_var}"
        )
    return url_parts.path
