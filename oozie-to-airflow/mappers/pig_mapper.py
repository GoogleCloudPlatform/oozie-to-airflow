import os

import jinja2
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from utils import el_utils, xml_utils


class PigMapper(ActionMapper):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(self, oozie_node, task_id, trigger_rule=TriggerRule.ALL_SUCCESS, params=None,
                 template='pig.tpl'):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self._parse_oozie_node(oozie_node)

    def _parse_oozie_node(self, oozie_node):
        self.resource_manager = el_utils.strip_el(self.oozie_node.find('resource-manager').text)
        self.name_node = el_utils.strip_el(self.oozie_node.find('name-node').text)
        self.script = el_utils.strip_el(self.oozie_node.find('script').text)
        self.params_dict = {}
        param_nodes = xml_utils.find_nodes_by_tag(oozie_node, 'param')
        if param_nodes:
            for node in param_nodes:
                param = el_utils.replace_el_with_var(node.text, params=self.params, quote=False)
                key, value = param.split('=')
                self.params_dict[key] = value
        self.properties = {}
        config = self.oozie_node.find('configuration')
        if config:
            property_nodes = xml_utils.find_nodes_by_tag(config, 'property')
            if property_nodes:
                for node in property_nodes:
                    name = node.find('name').text
                    value = el_utils.replace_el_with_var(node.find('value').text, params=self.params,
                                                         quote=False)
                    self.properties[name] = value

    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(
            searchpath=os.path.join(ROOT_DIR, 'templates/'))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template(self.template)
        return template.render(**self.__dict__)

    def convert_to_airflow_op(self):
        pass

    @staticmethod
    def required_imports():
        return ['from airflow.utils import dates',
                'from airflow.contrib.operators import dataproc_operator']
