# Copyright 2018 Google LLC
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

import re
import logging

from o2a_libs import el_basic_functions

FN_MATCH = re.compile(r'\${\s?(\w+)\(([\w\s,\'\"\-]*)\)\s?\}')
VAR_MATCH = re.compile(r'\${([\w.]+)}')

EL_CONSTANTS = {
    'KB': 1024 ** 1,
    'MB': 1024 ** 2,
    'GB': 1024 ** 3,
    'TB': 1024 ** 4,
    'PB': 1024 ** 5,
}

EL_FUNCTIONS = {
    'firstNotNull': el_basic_functions.first_not_null,
    'concat': el_basic_functions.concat,
    'replaceAll': el_basic_functions.replace_all,
    'appendAll': el_basic_functions.append_all,
    'trim': el_basic_functions.trim,
    'urlEncode': el_basic_functions.url_encode,
    'timestamp': el_basic_functions.timestamp,
    'toJsonStr': el_basic_functions.to_json_str,
    'toPropertiesStr': None,
    'toConfigurationStr': None,
}

WF_EL_FUNCTIONS = {
    'wf:id': None,
    'wf:name': None,
    'wf:appPath': None,
    'wf:conf': None,
    'wf:user': None,
    'wf:group': None,
    'wf:callback': None,
    'wf:transition': None,
    'wf:lastErrorNode': None,
    'wf:errorCode': None,
    'wf:errorMessage': None,
    'wf:run': None,
}


def strip_el(el):
    """
    Given an el function or variable like ${ variable },
    strips out everything except for the variable.
    """

    return re.sub('[${}]', '', el).strip()


def replace_el_with_var(el, params, quote=True):
    """
    Only supports a single variable
    """
    # Matches oozie EL variables e.g. ${hostname}
    var_match = VAR_MATCH.findall(el)

    jinjafied_el = el
    if var_match:
        for var in var_match:
            if var in params:
                jinjafied_el = jinjafied_el.replace('${' + var + '}',
                                                    params[var])
            else:
                logging.info('Couldn\'t replace EL {}'.format(var))

    return '\'' + jinjafied_el + '\'' if quote else jinjafied_el


def parse_el_func(el, el_func_map=None):
    # Finds things like ${ function(arg1, arg2 } and returns
    # a list like ['function', 'arg1, arg2']
    if el_func_map is None:
        el_func_map = EL_FUNCTIONS
    fn_match = FN_MATCH.findall(el)

    if not fn_match:
        return None

    # fn_match is of the form [('concat', "'ls', '-l'")]
    # for an el function like ${concat('ls', '-l')}
    if fn_match[0][0] not in el_func_map:
        raise KeyError('{} EL function not supported.'.format(fn_match[0][0]))

    mapped_func = el_func_map[fn_match[0][0]]

    func_name = mapped_func.__name__
    return '{}({})'.format(func_name, fn_match[0][1])


def convert_el_to_jinja(oozie_el, quote=True):
    """
    Converts an EL with either a function or a variable to the form:
    Variable:
        ${variable} -> {{ params.variable }}
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
    elif var_match:
        for var in var_match:
            jinjafied_el = jinjafied_el.replace(
                '${' + var + '}', '{{ params.' + var + ' }}')

    return '\'' + jinjafied_el + '\'' if quote else jinjafied_el


def parse_els(properties_file, prop_dict=None):
    """
    Parses the properties file into a dictionary, if the value has
    and EL function in it, it gets replaced with the corresponding
    value that has already been parsed. For example, a file like:

    job.properties
        host=user@google.com
        command=ssh ${host}

    The params would be parsed like:
        PARAMS = {
        host: 'user@google.com',
        command='ssh user@google.com',
    }
    """
    if prop_dict is None:
        prop_dict = {}
    if properties_file:
        with open(properties_file, 'r') as prop_file:
            for line in prop_file.readlines():
                if line.startswith('#') or line.startswith(' ') or line.startswith('\n'):
                    continue
                else:
                    props = [x.strip() for x in line.split('=', 1)]
                    prop_dict[props[0]] = replace_el_with_var(props[1],
                                                              prop_dict,
                                                              quote=False)

    return prop_dict
