#!/usr/bin/env bash

install(){
    # stacks
    if [ ! -d "/var/lib/ambari-server/resources/stacks/HDP/2.6/services/PRESTO" ];then
    echo "创建 /var/lib/ambari-server/resources/stacks/HDP/2.6/services/PRESTO"
    mkdir -p /var/lib/ambari-server/resources/stacks/HDP/2.6/services/PRESTO
    fi
    cp -R stacks/* /var/lib/ambari-server/resources/stacks/HDP/2.6/services/PRESTO/
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
