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
"Tests XML utils"
import unittest

from xml.etree import ElementTree as ET

from o2a.utils import xml_utils


class TestELUtils(unittest.TestCase):
    def test_find_node_by_name(self):
        doc = ET.Element("outer")
        node = ET.SubElement(doc, "inner_tag", attrib={"name": "test_attrib"})
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_node_by_name(element_tree.getroot(), "test_attrib")
        self.assertEqual(node, found)

    def test_find_node_by_name_not_found(self):
        doc = ET.Element("outer")
        node = ET.SubElement(doc, "inner_tag", attrib={"name": "test_attrib"})
        ET.SubElement(node, "in_inner_tag", attrib={"name": "out_of_scope"})
        element_tree = ET.ElementTree(doc)

        with self.assertRaises(xml_utils.NoNodeFoundException):
            xml_utils.find_node_by_name(element_tree.getroot(), "out_of_scope")

    def test_find_node_by_name_multiple(self):
        doc = ET.Element("outer")
        ET.SubElement(doc, "inner_tag", attrib={"name": "test_attrib"})
        ET.SubElement(doc, "other_inner_tag", attrib={"name": "test_attrib"})
        element_tree = ET.ElementTree(doc)

        with self.assertRaises(xml_utils.MultipleNodeFoundException):
            xml_utils.find_node_by_name(element_tree.getroot(), "test_attrib")

    def test_find_nodes_by_tag(self):
        doc = ET.Element("outer")
        node = ET.SubElement(doc, "tag1")
        ET.SubElement(doc, "tag2")
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_tag(element_tree.getroot(), "tag1")

        self.assertIn(node, found)
        self.assertEqual(1, len(found))

    def test_find_nodes_by_tag_none(self):
        doc = ET.Element("outer")
        ET.SubElement(doc, "tag1")
        ET.SubElement(doc, "tag2")
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_tag(element_tree.getroot(), "not_found")

        self.assertEqual(0, len(found))

    def test_find_nodes_by_tag_multiple(self):
        doc = ET.Element("outer")
        node1 = ET.SubElement(doc, "tag1")
        ET.SubElement(node1, "tag1")
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_tag(element_tree.getroot(), "tag1")

        self.assertEqual(1, len(found))
        # node2 out of scope.
        self.assertIn(node1, found)

    def test_find_nodes_by_attribute_found_no_tag(self):
        doc = ET.Element("outer")
        attribs = {"myattrib": "myname"}
        node1 = ET.SubElement(doc, "tag1", attrib=attribs)
        ET.SubElement(node1, "tag2", attrib=attribs)
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_attribute(
            root=element_tree.getroot(), attr="myattrib", val="myname", tag=None
        )

        self.assertEqual(1, len(found))
        # node2 attrib is out of scope.
        self.assertIn(node1, found)

    def test_find_nodes_by_attribute_found_tag(self):
        doc = ET.Element("outer")
        attribs = {"myattrib": "myname"}
        node1 = ET.SubElement(doc, "tag1", attrib=attribs)
        ET.SubElement(doc, "tag2", attrib=attribs)
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_attribute(
            root=element_tree.getroot(), attr="myattrib", val="myname", tag="tag1"
        )

        self.assertEqual(1, len(found))
        # node2 attrib is not under specified tag.
        self.assertIn(node1, found)

    def test_find_nodes_by_attribute_found_none(self):
        doc = ET.Element("outer")
        attribs = {"myattrib": "myname"}
        ET.SubElement(doc, "tag1", attrib=attribs)
        ET.SubElement(doc, "tag2", attrib=attribs)
        element_tree = ET.ElementTree(doc)

        found = xml_utils.find_nodes_by_attribute(
            root=element_tree.getroot(), attr="myattrib", val="wrongname", tag=None
        )

        # nodes attrib is incorrect
        self.assertEqual(0, len(found))
