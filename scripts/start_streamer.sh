python streamer.py `ifconfig eth0 | grep "inet addr" | awk -F: '{print $2}' | awk '{print $1}'` $1
