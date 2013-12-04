#USAGE: ./start_streamer <device:eth0|wlan0> <port>
python streamer.py `ifconfig $1 | grep "inet addr" | awk -F: '{print $2}' | awk '{print $1}'` $2
