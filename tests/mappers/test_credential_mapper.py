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
"""Tests decision_mapper"""
import unittest
from unittest import mock

from xml.etree import ElementTree as ET

from o2a.o2a_libs.property_utils import PropertySet
from o2a.converter.task import Task
from o2a.mappers.credentials_mapper import CredentialsMapper

from tests.utils.test_credential_extractor import EXAMPLE_XML_WITH_CREDENTIALS


class TestCredentialMapper(unittest.TestCase):
    def setUp(self):
        self.credentials_node = ET.fromstring(EXAMPLE_XML_WITH_CREDENTIALS).find("credentials")

    def test_create_mapper(self):
        self.maxDiff = None
        mapper = self._get_credentials_mapper()
        mapper.on_parse_node()
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(self.credentials_node, mapper.oozie_node)
        # test conversion from Oozie EL to Jinja
        self.assertEqual("hcaturi", mapper.credentials_extractor.hcat_metastore_uri)
        self.assertEqual(mapper.props.credentials_node_properties, mapper.credentials_extractor.credentials_properties)

    def _get_credentials_mapper(self):
        return CredentialsMapper(
            oozie_node=self.credentials_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            job_properties={},
            config={},
            props=PropertySet(),
        )
