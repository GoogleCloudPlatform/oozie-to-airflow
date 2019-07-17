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
"""
EL functions map module.
"""
import re
import o2a.o2a_libs.el_wf_functions as wf


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


def replace_all(src_string, regex, replacement):
    """
    Replace each occurrence of regular expression match in
    the first string with the replacement string and return the
    replaced string. A 'regex' string with null value is considered as
    no change. A 'replacement' string with null value is consider as an empty string.
    """
    if not regex:
        return src_string
    if not replacement:
        replacement = ""
    return re.sub(regex, replacement, src_string)


def append_all(src_str, append, delimiter):
    """
    Add the append string into each split sub-strings of the
    first string(=src=). The split is performed into src string
    using the delimiter . E.g. appendAll("/a/b/,/c/b/,/c/d/", "ADD", ",")
    will return /a/b/ADD,/c/b/ADD,/c/d/ADD. A append string with null
    value is consider as an empty string. A delimiter string with value null
    is considered as no append in the string.
    """
    if not delimiter:
        return src_str
    if not append:
        append = ""

    split_str = src_str.split(delimiter)
    appended_list = []
    for split in split_str:
        appended_list.append(split + append)
    return delimiter.join(appended_list)


def url_encode(src_str):
    """
    It returns the URL UTF-8 encoded value of the given string.
    A string with null value is considered as an empty string.
    """
    if not src_str:
        return ""
    import urllib.parse

    return urllib.parse.quote(src_str, encoding="UTF-8")


def timestamp():
    """
    It returns the UTC current date and time
    in W3C format down to the second (YYYY-MM-DDThh:mm:ss.sZ).
    i.e.: 1997-07-16T19:20:30.45Z
    """
    import datetime
    import pytz

    return datetime.datetime.now(pytz.utc).isoformat()


def to_json_str(py_map):
    import json

    return json.dumps(py_map)


def concat(str1: str, str2: str) -> str:
    """
    Returns the concatenation of 2 strings. A string
    with null value is considered as an empty string.
    """
    if str1 and str2:
        return "{} ~ {}".format(str1, str2)

    if not str1 and str2:
        return str2

    if str1 and not str2:
        return str1

    return ""


def trim(src_str: str) -> str:
    """
    It returns the trimmed value of the given string.
    A string with null value is considered as an empty string.
    """
    # May not behave like java, their documentation is unclear what
    # types of whitespace they strip.
    return str(src_str) + ".strip()" if src_str else ""


FUNCTION_MAP = {
    "wf_id": "run_id",
    "wf_name": "dag.dag_id",
    "timestamp": 'macros.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")',
    "wf_app_path": "params['nameNode']/user/params['userName']}/params['examplesRoot']}/apps/hive",
    "concat": concat,
    "trim": trim,
    "wf_conf": wf.conf,
}


def evaluate_function(name: str, args: tuple) -> str:
    func = FUNCTION_MAP.get(name, None)
    if func:
        if isinstance(func, str):
            return func
        return func(*args)  # type: ignore

    return ""
