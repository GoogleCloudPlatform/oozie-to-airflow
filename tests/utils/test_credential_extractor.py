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
"""Tests for param extractor"""
import unittest

from parameterized import parameterized

from xml.etree import ElementTree as ET

from o2a.converter.exceptions import ParseException
from o2a.utils.credential_extractor import CredentialExtractor

# language=XML
EXAMPLE_XML_WITHOUT_CREDENTIALS = """
<fragment>
    <param>INPUT1=VALUE1</param>
</fragment>
"""

# language=XML
EXAMPLE_XML_WITH_CREDENTIALS = """
<fragment>
  <credentials>
    <credential name="hive2" type="hive2">
      <property>
        <name>hive2.server.principal</name>
        <value>hiveprincipal</value>
      </property>
      <property>
        <name>hive2.jdbc.url</name>
        <value>hivejdbc</value>
      </property>
    </credential>
    <credential name="hcat" type="hcat">
      <property>
        <name>hcat.metastore.uri</name>
        <value>hcaturi</value>
      </property>
      <property>
        <name>hcat.metastore.principal</name>
        <value>hcatprincipal</value>
      </property>
    </credential>
  </credentials>
</fragment>
"""
EXAMPLE_CREDENTIALS = {
    "credentials": {
        "hive2": [{"hive2.server.principal": "hiveprincipal"}, {"hive2.jdbc.url": "hivejdbc"}],
        "hcat": [{"hcat.metastore.uri": "hcaturi"}, {"hcat.metastore.principal": "hcatprincipal"}],
    }
}


class CredentialExtractorModuleTestCase(unittest.TestCase):
    @parameterized.expand(
        [
            ({"oozie_node": ET.fromstring(EXAMPLE_XML_WITHOUT_CREDENTIALS).find("credentials")}, None, True),
            (
                {"oozie_node": ET.fromstring(EXAMPLE_XML_WITH_CREDENTIALS).find("credentials")},
                EXAMPLE_CREDENTIALS,
                False,
            ),
            ({"credentials_properties": {}}, None, True),
            ({"credentials_properties": EXAMPLE_CREDENTIALS}, EXAMPLE_CREDENTIALS, False),
        ]
    )
    def test_credentials_extractor_init(self, kwargs, expected_properties, is_error):
        if is_error:
            with self.assertRaises(ParseException):
                CredentialExtractor(**kwargs)
        else:
            result = CredentialExtractor(**kwargs)
            self.assertEqual(expected_properties, result.credentials_properties)
            if result.credentials_properties["credentials"] is not {}:
                self.assertEqual("hiveprincipal", result.hive_server_principal)
                self.assertEqual("hivejdbc", result.hive_jdbc_url)
                self.assertEqual("hcaturi", result.hcat_metastore_uri)
                self.assertEqual("hcatprincipal", result.hcat_metastore_principal)
