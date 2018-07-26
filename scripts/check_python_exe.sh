#!/bin/bash

ps_out=`ps -ef | grep $1 | grep -v 'grep' | grep -v $0`
result=$(echo $ps_out | grep "$1")
if [[ "$result" != "" ]];then
    echo "Running"
else
    /usr/local/bin/python3.6   /root/bewin_backend/scripts/"$1".py >> /dev/null &
fi

