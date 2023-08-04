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
"""PropertyParser"""
import os

from o2a.converter.workflow import Workflow
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.utils import el_utils
from o2a.utils.constants import CONFIG, JOB_PROPS


class PropertyParser:
    """
    Parse configuration.properties and job.properties to PropertySet
    """

    def __init__(self, workflow: Workflow, props: PropertySet):
        self.config_file = os.path.join(workflow.input_directory_path, CONFIG)
        self.job_properties_file = os.path.join(workflow.input_directory_path, JOB_PROPS)
        self.props = props

    def parse_property(self):
        self.read_and_update_job_properties_replace_el()
        self.read_config_replace_el()

    def read_config_replace_el(self):
        """
        Reads configuration properties to config dictionary.
        Replaces EL properties within.

        :return: None
        """
        self.props.config = el_utils.extract_evaluate_properties(
            properties_file=self.config_file, props=self.props
        )

    def read_and_update_job_properties_replace_el(self):
        """
        Reads job properties and updates job_properties dictionary with the read values
        Replaces EL job_properties within.

        :return: None
        """
        self.props.job_properties.update(
            el_utils.extract_evaluate_properties(properties_file=self.job_properties_file, props=self.props)
        )
