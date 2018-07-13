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

import uuid
import os.path as path
import sys
import os
import glob
import pwd
import grp
import signal
import time
from resource_management import *

from resource_management.libraries.script.script import Script
from resource_management.core.resources.system import Execute
from common import create_connectors, delete_connectors
from presto_client import smoketest_presto, PrestoClient


class Coordinator(Script):
    def install(self, env):
        import params

        # This allows us to access the params.presto_pid_file property as
        # format('{presto_pid_file}')
        env.set_params(params)

        # Install dependent packages
        self.install_packages(env)

        # Create user and group for Presto if they don't exist
        # try:
        #     grp.getgrnam(params.presto_group)
        # except KeyError:
        #     Group(group_name=params.presto_group)
        #
        # try:
        #     pwd.getpwnam(params.presto_user)
        # except KeyError:
        #     User(username=params.presto_user,
        #          gid=params.presto_group,
        #          groups=[params.presto_group],
        #          ignore_failures=True
        #          )
        try:
            Execute('groupadd {0}'.format(params.presto_group))
        except ExecutionFailed:
            print ""
        try:
            Execute('useradd -g {0} {1}'.format(params.presto_group, params.presto_user))
        except ExecutionFailed:
            print ""

        Execute('rm -rf {0} {1} {2}'.format(params.presto_base_dir, params.presto_log_dir, params.presto_pid_dir))
        # Create Presto directories
        Directory([params.presto_base_dir, params.presto_log_dir, params.presto_pid_dir, params.presto_node_data_dir, params.presto_catalog_dir],
                  mode=0755,
                  cd_access='a',
                  owner=params.presto_user,
                  group=params.presto_group,
                  create_parents=True
                  )

        # Create empty Presto install log
        File(params.presto_install_log,
             mode=0644,
             owner=params.presto_user,
             group=params.presto_group,
             content=''
             )
        # Download Presto
        Execute('cd {0}; wget {1} -O presto_server.tar.gz'.format(params.presto_base_dir, params.presto_server_download_url), user=params.presto_user)
        # install Presto
        Execute('cd {0};tar zxf presto_server.tar.gz --strip-components=1'.format(params.presto_base_dir), user=params.presto_user)

        # Ensure all files owned by presto user
        Execute('chown -R {0}:{1} {2}'.format(params.presto_user, params.presto_group, params.presto_base_dir))

        # create presto config directories
        Execute('mkdir -p {0}'.format(params.presto_catalog_dir), user=params.presto_user)

        # remove presto installation file
        Execute('cd {0}; rm presto_server.tar.gz'.format(params.presto_base_dir), user=params.presto_user)
        self.configure(env)

    def stop(self, env):
        import params
        Execute('cd {0}; bin/launcher stop'.format(params.presto_base_dir), user=params.presto_user)

    def start(self, env):
        import params

        self.configure(env)
        Execute('cd {0};bin/launcher start --launcher-config {1}/bin/launcher.properties\
         --data-dir {2}\
         --node-config {3}/node.properties\
         --jvm-config {4}/jvm.config\
         --config {5}/config.properties\
         --launcher-log-file {6}\
         --server-log-file {7}'
                .format(params.presto_base_dir,
                        params.presto_base_dir,
                        params.presto_node_data_dir,
                        params.presto_config_dir,
                        params.presto_config_dir,
                        params.presto_config_dir,
                        params.presto_launcher_log_file,
                        params.presto_server_log_file), user=params.presto_user)

        # create presto server pid file
        Execute('cat {0} > {1}'.format(params.presto_launcher_pid, params.presto_server_pid), user=params.presto_user)

        if 'presto_worker_hosts' in params.host_info.keys():
            all_hosts = params.host_info['presto_worker_hosts'] + \
                        params.host_info['presto_coordinator_hosts']
        else:
            all_hosts = params.host_info['presto_coordinator_hosts']
        smoketest_presto(
            PrestoClient('localhost', params.presto_user,
                         params.config_properties['http-server.http.port']),
            all_hosts)

    def status(self, env):
        import params
        env.set_params(params)
        check_process_status(params.presto_server_pid)

    def configure(self, env):
        from params import node_properties, jvm_config, config_properties, \
            config_directory, memory_configs, host_info, connectors_to_add, connectors_to_delete, presto_node_data_dir
        key_val_template = '{0}={1}\n'

        with open(path.join(config_directory, 'node.properties'), 'w') as f:
            for key, value in node_properties.iteritems():
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('node.id', str(uuid.uuid4())))
            f.write(key_val_template.format('node.data-dir', presto_node_data_dir))

        with open(path.join(config_directory, 'jvm.config'), 'w') as f:
            f.write(jvm_config['jvm.config'])

        with open(path.join(config_directory, 'config.properties'), 'w') as f:
            for key, value in config_properties.iteritems():
                if key == 'query.queue-config-file' and value.strip() == '':
                    continue
                if key in memory_configs:
                    value += 'GB'
                f.write(key_val_template.format(key, value))
            f.write(key_val_template.format('coordinator', 'true'))
            f.write(key_val_template.format('discovery-server.enabled', 'true'))

        create_connectors(node_properties, connectors_to_add)
        delete_connectors(node_properties, connectors_to_delete)
        # This is a separate call because we always want the tpch connector to
        # be available because it is used to smoketest the installation.
        create_connectors(node_properties, "{'tpch': ['connector.name=tpch']}")


if __name__ == '__main__':
    Coordinator().execute()
