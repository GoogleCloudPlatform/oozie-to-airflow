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
import logging
import os
import re

import jinja2

from definitions import ROOT_DIR
from o2a_libs import el_basic_functions, el_wf_functions

FN_MATCH = re.compile(r'\$\{\s?([\w\:]+)\(([\w\s,\'\"\-]*)\)\s?\}')
# Matches things like ${ var }
VAR_MATCH = re.compile(r'\${([\w\.]+)}')

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
    # WF_EL_FUNCTIONS
    'wf:id': el_wf_functions.wf_id,
    'wf:name': el_wf_functions.wf_name,
    'wf:appPath': el_wf_functions.wf_app_path,
    'wf:conf': el_wf_functions.wf_conf,
    'wf:user': el_wf_functions.wf_user,
    'wf:group': el_wf_functions.wf_group,
    'wf:callback': el_wf_functions.wf_callback,
    'wf:transition': el_wf_functions.wf_transition,
    'wf:lastErrorNode': el_wf_functions.wf_last_error_node,
    'wf:errorCode': el_wf_functions.wf_error_code,
    'wf:errorMessage': el_wf_functions.wf_error_message,
    'wf:run': el_wf_functions.wf_run,
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


def parse_el_func(el, el_func_map=EL_FUNCTIONS):
    METHOD_STR_IDX = 0
    METHOD_PARAM_IDX = 1

    # Finds things like ${ function(arg1, arg2 } and returns
    # a list like ['function', 'arg1, arg2']
    fn_match = FN_MATCH.findall(el)

    if not fn_match:
        return None

    parsed_fns = []
    for fn in fn_match:
        # fn_match is of the form [('concat', "'ls', '-l'")]
        # for an el function like ${concat('ls', '-l')}
        if fn[METHOD_STR_IDX] not in el_func_map:
            raise KeyError('{} EL function not supported.'.format(fn[METHOD_STR_IDX]))

        mapped_func = el_func_map[fn[METHOD_STR_IDX]]

        func_name = mapped_func.__name__
        parsed_fns.append('{}({})'.format(func_name, fn[METHOD_PARAM_IDX]))

    return parsed_fns


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

    if var_match:
        for var in var_match:
            jinjafied_el = jinjafied_el.replace(
                '${' + var + '}', '{{ params.' + var + ' }}')
    if fn_match:
        template_loader = jinja2.FileSystemLoader(
            searchpath=os.path.join(ROOT_DIR, 'templates/'))
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template('el.tpl')

        subbed_str = re.sub(FN_MATCH, '{}', jinjafied_el)
        # Convert to python o2a_libs functions
        parsed_fns = parse_el_func(jinjafied_el, EL_FUNCTIONS)
        return template.render(str=subbed_str, els=parsed_fns)

    # This takes care of any escaping that must be done for python to understand
    return repr(jinjafied_el) if quote else repr(jinjafied_el)[1:-1]


def parse_els(properties_file, prop_dict={}):
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
