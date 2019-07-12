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
"""Base transformer classes"""

from o2a.converter.workflow import Workflow


from o2a.o2a_libs.property_utils import PropertySet


class BaseWorkflowTransformer:
    """
    Base class for all transformers
    """

    def process_workflow_after_parse_workflow_xml(self, workflow: Workflow):
        pass

    def process_workflow_after_convert_nodes(self, workflow: Workflow, props: PropertySet):
        pass
