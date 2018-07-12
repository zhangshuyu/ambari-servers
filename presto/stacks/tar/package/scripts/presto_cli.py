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
import grp
import pwd


from resource_management.core.exceptions import ClientComponentHasNoStatus
from resource_management.core.resources.system import Execute
from resource_management.libraries.script.script import Script


class Cli(Script):
    def install(self, env):
        import params

        # Install dependent packages
        self.install_packages(env)

        # Create user and group for Presto if they don't exist
        try:
            grp.getgrnam(params.presto_group)
        except KeyError:
            Group(group_name=params.presto_group)

        try:
            pwd.getpwnam(params.presto_user)
        except KeyError:
            User(username=params.presto_user,
                 gid=params.presto_group,
                 groups=[params.presto_group],
                 ignore_failures=True
                 )

        Execute('rm -rf {0}/bin/presto-cli'.format(params.presto_base_dir), user=params.presto_user)
        # Create Presto directories
        Directory([params.presto_base_dir, params.presto_log_dir, params.presto_pid_dir],
                  mode=0755,
                  cd_access='a',
                  owner=params.presto_user,
                  group=params.presto_group,
                  create_parents=True
                  )

        Execute('wget --no-check-certificate {0} -O {1}/bin/presto-cli'.format(params.presto_cli_download_url, params.presto_base_dir), user=params.presto_user)
        Execute('chmod +x {0}/bin/presto-cli'.format(params.presto_base_dir), user=params.presto_user)

    def status(self, env):
        raise ClientComponentHasNoStatus()

    def configure(self, env):
        import params
        env.set_params(params)

    def start(self, env):
        import params
        env.set_params(params)

    def stop(self, env):
        import params
        env.set_params(params)

if __name__ == '__main__':
    Cli().execute()