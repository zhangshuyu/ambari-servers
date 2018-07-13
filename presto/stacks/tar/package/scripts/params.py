#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from resource_management.libraries.script.script import Script

# config object that holds the configurations declared in the config xml file
config = Script.get_config()

# config for env
presto_user = config['configurations']['env.properties']['presto_user']
presto_group = config['configurations']['env.properties']['presto_group']
presto_base_dir = config['configurations']['env.properties']['presto_base_dir']
presto_log_dir = config['configurations']['env.properties']['presto_log_dir']
presto_pid_dir = config['configurations']['env.properties']['presto_pid_dir']
presto_server_download_url = config['configurations']['env.properties']['presto_server_download_url']
presto_cli_download_url = config['configurations']['env.properties']['presto_cli_download_url']
presto_server_log_file = presto_log_dir + '/presto-server.log'
presto_launcher_log_file = presto_log_dir + '/launcher.log'
presto_install_log = presto_base_dir + 'presto-install.log'

# config for node
presto_node_data_dir = presto_base_dir + "/data"
presto_catalog_dir = presto_base_dir + "/etc/catalog"
presto_plugin_dir = presto_base_dir + "/plugin"
presto_launcher_pid = presto_node_data_dir + '/var/run/launcher.pid'
presto_server_pid = presto_pid_dir + '/presto.pid'

node_properties = config['configurations']['node.properties']
jvm_config = config['configurations']['jvm.config']
config_properties = config['configurations']['config.properties']

connectors_to_add = config['configurations']['connectors.properties']['connectors.to.add']
connectors_to_delete = config['configurations']['connectors.properties']['connectors.to.delete']

daemon_control_script = '/etc/init.d/presto'
#config_directory = '/etc/presto'
config_directory = presto_base_dir + '/etc'

memory_configs = ['query.max-memory-per-node', 'query.max-memory']

host_info = config['clusterHostInfo']

host_level_params = config['hostLevelParams']
java_home = host_level_params['java_home']

