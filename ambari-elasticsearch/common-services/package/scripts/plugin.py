#!/usr/bin/env python

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import os
import glob
import pwd
import grp
import signal
import time
from resource_management import *
from elastic_common import kill_process

class Slave(Script):

    def install(self, env):
        import params

        env.set_params(params)

        # Create Elasticsearch directories
        Directory([params.elastic_web_plugin_server_base_dir],
                  mode=0755,
                  cd_access='a',
                  owner=params.elastic_user,
                  group=params.elastic_group,
                  create_parents=True
                  )

        # Create empty Elasticsearch install log
        File(params.elastic_web_plugin_server_install_log,
             mode=0644,
             owner=params.elastic_user,
             group=params.elastic_group,
             content=''
             )

        # Download Elasticsearch Plugin
        cmd = format("cd {elastic_web_plugin_server_base_dir}; wget {elastic_web_url} -O elasticsearch-web-plugin.zip -a {elastic_web_plugin_server_install_log}")
        Execute(cmd, user=params.elastic_user)

        # Install Elasticsearch
        cmd = format("cd {elastic_web_plugin_server_base_dir}; unzip elasticsearch-web-plugin.zip")
        Execute(cmd, user=params.elastic_user)

        # Ensure all files owned by elasticsearch user
        cmd = format("chown -R {elastic_user}:{elastic_group} {elastic_web_plugin_server_base_dir}")
        Execute(cmd)

        # Remove Elasticsearch installation file
        cmd = format("cd {elastic_web_plugin_server_base_dir}; rm elasticsearch-web-plugin.zip")
        Execute(cmd, user=params.elastic_user)

        Execute('echo "Install complete"')
        
        #if os.path.exists(format("{es_plugin_sites_home}/{es_plugin_sites_name}")):
        #    Execute(format("rm -rf {es_plugin_sites_home}/{es_plugin_sites_name}"))
        #Execute(format(" wget -O {es_plugin_sites_home}/elastic.zip {params.elastic_web_url}"), user="root")
        #Execute(format(' unzip -o {es_plugin_sites_home}/{es_plugin_sites_name}.zip  -d {es_plugin_sites_home} >>/etc/null'), user="root")
        #Execute(format(' rm -rf {es_plugin_sites_home}/{es_plugin_sites_name}.zip'), user="root")
        

    def configure(self, env):
        import params

        env.set_params(params)

        configurations = params.config['configurations']['elastic-plugin']

        File(format("{elastic_web_plugin_server_base_dir}/es-site/site-server/site_configuration.json"),
             content=Template("elasticsearch_plugin_config.json.j2",
                              configurations=configurations),
             owner="root",
             group="root"
            )


    def stop(self, env):
        # Import properties defined in -config.xml file from the params class
        import params

        # Import properties defined in -env.xml file from the status_params class
        import status_params

        # This allows us to access the params.elastic_pid_file property as
        env.set_params(params)

        #Execute("kill ` ps -ax |grep node-server.js | grep -v grep |awk '{print $1}' `")
        kill_process(params.elastic_web_plugin_server_pid_file, params.elastic_user, params.elastic_log_dir)


    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)

        Execute(format("cd {elastic_web_plugin_server_base_dir}/elasticsearch-web-plugin/es-site/site-server;./start.sh"))
        #time.sleep(5)
        #site_server_pid = "` ps -ax |grep node-server.js | grep -v grep |awk '{print $1}' `"
        #cmd=format("echo {site_server_pid} >{params.site_server_pid_file}")
        #Execute(cmd)


    def status(self, env):
        # Import properties defined in -env.xml file from the status_params class
        import status_params

        # This allows us to access the params.elastic_pid_file property as
        #  format('{elastic_pid_file}')
        env.set_params(status_params)
        check_process_status(status_params.elastic_web_plugin_server_pid_file)


if __name__ == "__main__":
    Slave().execute()
