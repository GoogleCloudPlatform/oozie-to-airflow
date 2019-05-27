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
from typing import Dict, Iterator, Any
from collections.abc import Mapping
import json


# pylint: disable=too-few-public-methods
class PropertySet(Mapping):
    """Holds all the different types of properties (job/action node for now - job.xml and workflow.xml in
       the future) and implements [] operator to return property value according to the oozie algorithm
       of property precedence.

       Note that the configuration_properties are not used in the [] operator nor in the
       job_properties_merged. You need to access the configuration properties
       via explicit <PROPERTY_SET>.configuration_properties['key']
    """

    def __iter__(self) -> Iterator[str]:
        return self.job_properties_merged.__iter__()

    def __len__(self) -> int:
        return self.job_properties_merged.__len__()

    def __init__(
        self,
        configuration_properties: Dict[str, str],
        job_properties: Dict[str, str],
        action_node_properties: Dict[str, str] = None,
    ):
        self.configuration_properties: Dict[str, str] = configuration_properties
        self.job_properties: Dict[str, str] = job_properties
        self.action_node_properties: Dict[str, str] = action_node_properties or {}

    def __getitem__(self, item: str) -> str:
        return self.job_properties_merged[item]

    def __contains__(self, key: Any) -> bool:
        return key in self.job_properties_merged

    @property
    def job_properties_merged(self) -> Dict[str, str]:
        # not optimal but allows to modify properties in job.properties/action_node_properties at any time
        job_properties_merged: Dict[str, str] = {}
        job_properties_merged.update(self.job_properties)
        job_properties_merged.update(self.action_node_properties)
        return job_properties_merged

    def __repr__(self) -> str:
        return (
            f"PropertySet(configuration_properties={json.dumps(self.configuration_properties, indent=2)}, "
            f"job_properties={json.dumps(self.job_properties, indent=2)}, "
            f"action_node_properties={json.dumps(self.action_node_properties, indent=2)}, "
            f"job_properties_merged={json.dumps(self.job_properties_merged, indent=2)})"
        )
