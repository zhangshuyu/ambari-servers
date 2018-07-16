#!/usr/bin/env bash

install(){
    # stacks
    if [ ! -d "/var/lib/ambari-server/resources/stacks/HDP/2.6/services/FLINK" ];then
    echo "创建 /var/lib/ambari-server/resources/stacks/HDP/2.6/services/FLINK"
    mkdir -p /var/lib/ambari-server/resources/stacks/HDP/2.6/services/FLINK
    fi
    cp -R conf/* /var/lib/ambari-server/resources/stacks/HDP/2.6/services/FLINK/
}

checkUser(){
    if [ `whoami` != "root" ];then
        echo "only root can run me"
        exit 1
    else
        install
    fi
}

checkUser
