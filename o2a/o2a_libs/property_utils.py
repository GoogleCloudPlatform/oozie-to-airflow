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

"""Stores property set for use in particular actions"""
from typing import Dict
import json


# pylint: disable=too-few-public-methods
class PropertySet:
    """Holds all the different types of properties (job/action node for now - job.xml and workflow.xml in
       the future) and implements [] operator to return property value according to the Oozie algorithm
       of property precedence.

       Note that the config are not used in the [] operator nor in the
       merged. You need to access the configuration properties
       via explicit <PROPERTY_SET>.config['key']
    """

    def __init__(
        self,
        job_properties: Dict[str, str] = None,
        config: Dict[str, str] = None,
        action_node_properties: Dict[str, str] = None,
    ):
        self.job_properties: Dict[str, str] = job_properties or {}
        self.config: Dict[str, str] = config or {}
        self.action_node_properties: Dict[str, str] = action_node_properties or {}

    @property
    def merged(self) -> Dict[str, str]:
        """
        Those are merged job and action node properties.
        :return:
        """
        # not optimal but allows to modify properties in job.properties/action_node_properties at any time
        merged_props: Dict[str, str] = {}
        merged_props.update(self.job_properties)
        merged_props.update(self.action_node_properties)
        return merged_props

    def __repr__(self) -> str:
        return (
            f"PropertySet(config={json.dumps(self.config, indent=2)}, "
            f"job_properties={json.dumps(self.job_properties, indent=2)}, "
            f"action_node_properties={json.dumps(self.action_node_properties, indent=2)})"
        )

    def __eq__(self, other):
        return (
            isinstance(other, PropertySet)
            and self.config == other.config
            and self.job_properties == other.job_properties
            and self.action_node_properties == other.action_node_properties
        )
