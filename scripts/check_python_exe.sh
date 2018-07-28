#!/bin/bash

ps_out=`ps -ef | grep $1 | grep -v 'grep' | grep -v $0`
result=$(echo $ps_out | grep "$1")
if [[ "$result" != "" ]];then
    echo "Running"
else
    cd /root/bewin_backend/scripts/
    /usr/local/bin/python3   "$1".py >> /dev/null &
fi

