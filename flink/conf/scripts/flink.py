import os
import time
from resource_management import *
from resource_management.core.logger import Logger


class Master(Script):
  def install(self, env):

    import params

    service_packagedir = os.path.realpath(__file__).split('/scripts')[0]
            
    Directory([params.flink_pid_dir, params.flink_log_dir, params.flink_install_dir],
              owner=params.flink_user,
              group=params.flink_group
              )

    File(params.flink_log_file,
         mode=0644,
         owner=params.flink_user,
         group=params.flink_group,
         content=''
         )
    

    # Execute('echo Installing packages')
    # Execute('yum -y install flink-dist_2.11', user="root")
    # Download flink
    Execute('cd {0}; wget {1} -O flink.tar.gz '.format(params.flink_install_dir, params.flink_download_url), user=params.flink_user)
    # Install flink
    Execute('cd {0}; tar -xf flink.tar.gz --strip-components=1'.format(params.flink_install_dir), user=params.flink_user)
    # remove flink install tarball
    Execute('cd {0}; rm -rf flink.tar.gz'.format(params.flink_install_dir), user=params.flink_user)
    self.configure(env, True)
    


  def configure(self, env, isInstall=False):
    import params
    import status_params
    env.set_params(params)
    env.set_params(status_params)
    
    self.set_conf_bin(env)
        
    properties_content=InlineTemplate(params.flink_yaml_content)
    File(format("{conf_dir}/flink-conf.yaml"), content=properties_content, owner=params.flink_user)
        
    
  def stop(self, env):
    import params
    import status_params    
    from resource_management.core import sudo
    if params.flink_start_on_yarn:
        Logger.info("flink stop on yarn")
        pid = str(sudo.read_file(status_params.flink_pid_file))
        Execute('yarn application -kill ' + pid, user=params.flink_user)
        Execute('rm ' + status_params.flink_pid_file, ignore_failures=True)
    else:
        Logger.info("flink stop cluster")
        Execute ("/usr/hdp/2.5.0.0-1245/flink/bin//stop-cluster.sh" , user="root", logoutput=True)
        cmd=format("rm -rf {status_params.jobmanager_pid_file}")
        Execute(cmd)

    
 
      
  def start(self, env):
    import params
    import status_params
    self.set_conf_bin(env)  
    self.configure(env) 
    
    self.create_hdfs_user(params.flink_user)
    
    Directory(params.flink_pid_dir,
              owner=params.flink_user,
              group=params.flink_group
              )

    if params.flink_start_on_yarn:
        Logger.info("flink start on yarn")
        
        Execute('echo bin dir ' + params.bin_dir)        
        Execute('echo pid file ' + status_params.flink_pid_file)
        cmd = format("export HADOOP_CONF_DIR={hadoop_conf_dir}; {bin_dir}/yarn-session.sh -n {flink_numcontainers} -s {flink_numberoftaskslots} -jm {flink_jobmanager_memory} -tm {flink_container_memory} -qu {flink_queue} -nm {flink_appname} -d")
        Execute (cmd, user=params.flink_user, logoutput=True)
        Execute("yarn application -list 2>/dev/null | awk '/" + params.flink_appname + "/ {print $1}' | head -n1 > " + status_params.flink_pid_file, user=params.flink_user)
    else:
        Logger.info("flink start cluster")
        Execute (format("{bin_dir}/start-cluster.sh") , user="root", logoutput=True)
        ##out put pid file
        time.sleep(5)
        flink_pid = "` ps -ax |grep org.apache.flink.runtime.jobmanager.JobManager | grep -v grep |awk '{print $1}' `"
        cmd=format("echo {flink_pid} >{status_params.jobmanager_pid_file}")
        Execute(cmd)


  def check_flink_status(self, pid_file):
    from resource_management.core.exceptions import ComponentIsNotRunning
    from resource_management.core import sudo
    from subprocess import PIPE,Popen
    import shlex
    if not os.path.exists(pid_file) or not os.path.isfile(pid_file):
      raise ComponentIsNotRunning()
    try:
      pid = str(sudo.read_file(pid_file)) 
      cmd_line = "/usr/bin/yarn application -list"
      args = shlex.split(cmd_line)
      proc = Popen(args, stdout=PIPE)
      p = str(proc.communicate()[0].split())
      if p.find(pid.strip()) < 0:
        raise ComponentIsNotRunning() 
    except Exception, e:
      raise ComponentIsNotRunning()


  def status(self, env):
    import status_params
    if status_params.flink_start_on_yarn:
        Logger.info("params.flink_start_on_yarn = true")
        self.check_flink_status(status_params.flink_pid_file)
    else:
        Logger.info("params.flink_start_on_yarn = false")
        env.set_params(status_params)
        check_process_status(status_params.jobmanager_pid_file)

  def set_conf_bin(self, env):
    import params
    params.conf_dir = params.flink_install_dir + '/conf'
    params.bin_dir = params.flink_install_dir + '/bin'
#     if params.setup_prebuilt:
#       params.conf_dir =  params.flink_install_dir+ '/conf'
#       params.bin_dir =  params.flink_install_dir+ '/bin'
#     else:
#       params.conf_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/conf')[0]
#       params.bin_dir =  glob.glob(params.flink_install_dir+ '/flink-dist/target/flink-*/flink-*/bin')[0]
    

  def create_hdfs_user(self, user):
    Execute('hadoop fs -mkdir -p /user/'+user, user='hdfs', ignore_failures=True)
    Execute('hadoop fs -chown ' + user + ' /user/'+user, user='hdfs')
    Execute('hadoop fs -chgrp ' + user + ' /user/'+user, user='hdfs')
          
if __name__ == "__main__":
  Master().execute()
