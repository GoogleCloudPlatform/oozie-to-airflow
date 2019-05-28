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
"""Template utilities"""
from typing import Dict, Any

import jinja2

from o2a.definitions import TPL_PATH
from o2a.utils import python_serializer

from o2a.utils.variable_name_utils import convert_to_python_variable

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath=TPL_PATH)
TEMPLATE_ENV = jinja2.Environment(
    loader=TEMPLATE_LOADER, undefined=jinja2.StrictUndefined, trim_blocks=True, lstrip_blocks=True
)
TEMPLATE_CACHES: Dict[str, Any] = {}

TEMPLATE_ENV.filters["to_var"] = convert_to_python_variable

TEMPLATE_ENV.filters["to_python"] = python_serializer.serialize


def render_template(template_name: str, *args, **kwargs) -> str:
    """Render Jinja template"""
    if template_name not in TEMPLATE_CACHES:
        template = TEMPLATE_ENV.get_template(template_name)
        TEMPLATE_CACHES[template_name] = template
    content: str = TEMPLATE_CACHES[template_name].render(*args, **kwargs)
    return content
