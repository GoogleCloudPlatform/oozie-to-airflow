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
"""Tests for property utils"""

from unittest import TestCase

from o2a.o2a_libs.property_utils import PropertySet


class TestPropertySet(TestCase):
    def test_xml_escaping(self):
        # Given
        pset = PropertySet(
            job_properties={"url": "http://example.com:8080/workflow?job-id=$jobId&status=$status"},
            action_node_properties={"my.injection.attempt": "<value>1</value>"},
        )
        # When
        pset_escaped = pset.xml_escaped.merged

        # Then
        self.assertEqual(
            "http://example.com:8080/workflow?job-id=$jobId&amp;status=$status", pset_escaped["url"]
        )
        self.assertEqual("&lt;value&gt;1&lt;/value&gt;", pset_escaped["my.injection.attempt"])
