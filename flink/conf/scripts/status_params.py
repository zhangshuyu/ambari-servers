#!/usr/bin/env python
from resource_management import *
import sys, os

config = Script.get_config()

flink_pid_dir=config['configurations']['flink-env']['flink_pid_dir']
flink_pid_file=flink_pid_dir + '/flink.pid'

flink_start_on_yarn=True

jobmanager_pid_file=flink_pid_dir + '/jobmanager.pid'
taskmanager_pid_file=flink_pid_dir + '/taskmanager.pid'


