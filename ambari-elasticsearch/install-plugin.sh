#!/usr/bin/env bash

install(){
    # stacks
    rm -rf /var/lib/ambari-server/resources/stacks/HDP/2.6/services/ELASTICSEARCH
    mkdir /var/lib/ambari-server/resources/stacks/HDP/2.6/services/ELASTICSEARCH
    cp -R stacks/* /var/lib/ambari-server/resources/stacks/HDP/2.6/services/ELASTICSEARCH/

    # common-services
    rm -rf /var/lib/ambari-server/resources/common-services/ELASTICSEARCH
    mkdir -p /var/lib/ambari-server/resources/common-services/ELASTICSEARCH/6.3.0
    cp -R common-services/* /var/lib/ambari-server/resources/common-services/ELASTICSEARCH/6.3.0
}

checkUser(){
    if [ `whoami` != "root" ];then
        echo " only root can run me"
        exit 1
    else
        install
    fi
}

checkUser
