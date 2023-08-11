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
"""XML parsing utilities"""
from typing import List, Optional, cast
from xml.etree import ElementTree as ET
from o2a.o2a_libs.src.o2a_lib import el_parser


class NoNodeFoundException(Exception):
    pass


class MultipleNodeFoundException(Exception):
    pass


def find_node_by_name(root, name) -> ET.Element:
    """
    Find a node with an attribute 'name' the same as the passed in parameter
    name. Since we are refining by name there should only be one (1) node with
    the name per workflow based on the root directory.

    :param root: The node of which to look under for the node name. Only looks
        at direct descendants -- not all descendants.
    :param name: Name of node to look for.
    :return: The XML node that was found, or raises an exception if not found.
    """
    node = find_nodes_by_attribute(root, "name", name)

    if not node:
        raise NoNodeFoundException(f"Node with name {name} not found.")
    if len(node) > 1:
        raise MultipleNodeFoundException(f"More than one node with name {name} found")
    return node[0]


def find_node_by_tag(root, tag) -> Optional[ET.Element]:
    """
    Returns a first XML node that have the tag provided. In this case
    only direct descendants under the root node are checked for the tag.
    If nothing is found, it returns None.
    """
    nodes = find_nodes_by_tag(root, tag)
    if nodes:
        return nodes[0]
    return None


def find_nodes_by_tag(root, tag) -> List[ET.Element]:
    """
    Returns a list of XML nodes that have the tag provided. In this case
    only direct descendants under the root node are checked for the tag.
    """
    return cast(List[ET.Element], root.findall("." + tag))


def find_nodes_by_attribute(root, attr, val, tag=None) -> List[ET.Element]:
    """
    Finds node with the attribute `attr` matching `val`. An optional tag can be
    specified which will narrow down the search space to only the tag passed in.

    :param root: Node's direct descendants to look under.
    :param attr: Attribute to match with `val`
    :param val: Required value of attribute
    :param tag: Optional, can decrease the search space even more.
    :return: List of matching XML nodes.
    """
    matching_nodes: List[ET.Element] = []
    search_space = find_nodes_by_tag(root, tag) if tag else root

    for node in search_space:
        if attr in node.attrib and node.attrib[attr] == val:
            matching_nodes.append(node)
    return matching_nodes


def get_tag_el_text(root: ET.Element, tag: str, default: str = None) -> Optional[str]:
    """
    If a node exists in the oozie_node with the tag specified in tag, it
    will attempt to replace the EL (if it exists) with the corresponding
    variable. If no EL var is found, it just returns text. However, if the
    tag is not found under oozie_node, then return default. If there are
    more than one with the specified tag, it uses the first one found.
    """
    var = find_node_by_tag(root, tag)
    if var is not None and var.text is not None:
        # Only check the first one
        return el_parser.translate(var.text)
    return default


def get_tags_el_array_from_text(root: ET.Element, tag: str) -> List[str]:
    """
    If nodes exist in the oozie_node with the tag specified in tag, it
    will build an array of text values for all matching nodes. While doing it
    it will attempt to resolve EL expressions in the text values.
    """
    tags_array = []
    node_array = find_nodes_by_tag(root=root, tag=tag)
    if node_array:
        for node in node_array:
            if node.text is not None:
                tags_array.append(el_parser.translate(node.text))
    return tags_array
