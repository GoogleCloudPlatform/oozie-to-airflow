# Copyright 2018 Google LLC
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


class NoNodeFoundException(Exception):
    pass


class MultipleNodeFoundException(Exception):
    pass


def find_node_by_name(root, name):
    """
    Find a node with an attribute 'name' the same as the passed in parameter
    name. Since we are refining by name there should only be one (1) node with
    the name per workflow based on the root directory.

    :param root: The node of which to look under for the node name. Only looks
        at direct descendants -- not all descendants.
    :param name: Name of ndoe to look for.
    :return: The XML node that was found, or raises an exception if not found.
    """
    node = find_nodes_by_attribute(root, 'name', name)

    if len(node) == 0:
        raise NoNodeFoundException("Node with name {} not found.".format(name))
    elif len(node) > 1:
        raise MultipleNodeFoundException(
            "More than one node with name {} found".format(name))
    else:
        return node[0]


def find_nodes_by_tag(root, tag):
    """
    Returns a list of XML nodes that have the tag provided. In this case
    only direct descendants under the root node are checked for the tag.
    """
    return root.findall('.' + tag)


def find_nodes_by_attribute(root, attr, val, tag=None):
    """
    Finds node with the attribute `attr` matching `val`. An optional tag can be
    specified which will narrow down the search space to only the tag passed in.

    :param root: Node's direct descendants to look under.
    :param attr: Attribute to match with `val`
    :param val: Required value of attribute
    :param tag: Optional, can decrease the search space even more.
    :return: List of matching XML nodes.
    """
    matching_nodes = []
    search_space = find_nodes_by_tag(root, tag) if tag else root

    for node in search_space:
        if attr in node.attrib and node.attrib[attr] == val:
            matching_nodes.append(node)
    return matching_nodes
