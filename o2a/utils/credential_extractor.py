"""Extract credentials from oozie's credentials node"""
import logging
from typing import Dict, List, Optional
from xml.etree.ElementTree import Element

from o2a.converter.exceptions import ParseException

TAG_CREDENTIALS = "credentials"
TAG_CREDENTIAL = "credential"
TAG_PROPERTY = "property"
TAG_NAME = "name"
TAG_VALUE = "value"
TAG_HCAT = "hcat"
TAG_HIVE = "hive2"
TAG_HCAT_METASTORE_URI = "hcat.metastore.uri"
TAG_HCAT_PRINCIPAL_NAME = "hcat.metastore.principal"
TAG_HIVE_PRINCIPAL_NAME = "hive2.server.principal"
TAG_HIVE_JDBC_URL = "hive2.jdbc.url"


class CredentialExtractor:
    def __init__(self, oozie_node: Optional[Element] = None, credentials_properties: Optional[Dict[str, str]] = None):
        self._oozie_node = oozie_node or None
        self._credentials_properties = credentials_properties or None

        logging.warning(f"self._oozie_node: {self._oozie_node}")
        logging.warning(f"self._credentials_properties: {self._credentials_properties}")

        if not self._oozie_node and not self._credentials_properties:
            raise ParseException("Either oozie_node or credentials_properties must be provided")

    @property
    def hcat_metastore_uri(self) -> str:
        return self._get_cred_value(TAG_HCAT_METASTORE_URI, TAG_HCAT)

    @property
    def hcat_principal_name(self) -> str:
        return self._get_cred_value(TAG_HCAT_PRINCIPAL_NAME, TAG_HCAT)

    @property
    def hive_jdbc_url(self):
        return self._get_cred_value(TAG_HIVE_JDBC_URL, TAG_HIVE)

    @property
    def hive_server_principal(self):
        return self._get_cred_value(TAG_HIVE_PRINCIPAL_NAME, TAG_HIVE)

    @property
    def credentials_properties(self) -> Dict[str, str]:
        if self._credentials_properties is None:
            self._parse_config()
        return self._credentials_properties

    def _get_cred_value(self, cred_name: str, cred_type: str) -> str:
        return next(
            (
                el[cred_name]
                for el in self.credentials_properties.get("credentials").get(cred_type)
                if el.get(cred_name)
            ),
            None,
        )

    def _parse_config(self):
        self._credentials_properties = self._extract_credentials_from_credentials_node(self._oozie_node)

    @staticmethod
    def _extract_credentials_from_credentials_node(credentials_node: List[Element]) -> Dict[str, str]:
        """Extracts credentials properties from ``credentials`` node"""
        credentials_dict: Dict[str, str] = dict()
        for credential_node in credentials_node.findall(TAG_CREDENTIAL):
            if not credential_node:
                raise ParseException(
                    'Element "credential" should have direct children element(s): property. Make sure the '
                    "credentials element is valid."
                )

            credential_name = credential_node.attrib[TAG_NAME]
            if credential_name in credentials_dict:
                raise ParseException(f"Duplicate credential name: {credential_name}")
            else:
                credentials_dict[credential_name] = []

            for property in credential_node.findall(TAG_PROPERTY):
                name_node = property.find(TAG_NAME)
                value_node = property.find(TAG_VALUE)
                if name_node is None or value_node is None:
                    raise ParseException(
                        'Element "property" should have direct children elements: name, value. One of them does not '
                        "exist. Make sure the configuration element is valid."
                    )

                name = name_node.text
                value = value_node.text
                if not name or not value:
                    raise ParseException(
                        'Element "name" and "value" should have content, hoever of them is empty. '
                        "Make sure the configuration element is valid."
                    )

                credentials_dict[credential_name] = credentials_dict[credential_name] + [{name: value}]

        return {"credentials": credentials_dict}
