import logging
from typing import List, Optional, Set, Tuple

from xml.etree.ElementTree import Element

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.xml_utils import find_nodes_by_tag
from o2a.utils.credential_extractor import CredentialExtractor


class CredentialsMapper(BaseMapper):
    def __init__(self, oozie_node: Element, name: str, dag_name: str, props: Optional[PropertySet] = None, **kwargs):
        super().__init__(oozie_node=oozie_node, name=name, dag_name=dag_name, props=props, **kwargs)
        self._credentials_extractor: CredentialExtractor = CredentialExtractor(oozie_node=oozie_node)

    def on_parse_node(self):
        super().on_parse_node()
        self._parse_config()

    @property
    def credentials_extractor(self):
        return self._credentials_extractor

    @property
    def mapper(self):
        return 'credentials'

    def to_tasks_and_relations(self):
        pass

    def required_imports(self):
        pass

    def _parse_config(self):
        credentials_extractor = CredentialExtractor(self.oozie_node)
        self.props.credentials_node_properties = credentials_extractor.credentials_properties
