#!/bin/sh

SERVICE="main.py"

if ! ps -ef | grep "$SERVICE" | grep -v grep >/dev/null
then
    python main.py >/dev/null 2>&1
fi
