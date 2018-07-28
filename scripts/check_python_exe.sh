#!/bin/bash

ps_out=`ps -ef | grep $1 | grep -v 'grep' | grep -v $0`
result=$(echo $ps_out | grep "$1")
if [[ "$result" == "" ]];then
    echo "[`date +%Y-%m-%d,%H:%m:%s`] $1 down!" >> /root/bewin_backend/logs/check.log
    cd /root/bewin_backend/scripts/
    /usr/local/bin/python3   "$1" >> /dev/null &
fi

