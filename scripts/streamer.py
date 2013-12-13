#!/usr/bin/python
import time
import sys
import socket    

arglen=len(sys.argv)
if arglen<3:
    print('Read file for instructions. Please include port as a parameter')
    exit()

s = socket.socket()
s.bind(('', int(sys.argv[2])))
s.listen(5)

while True:
    print('Waiting for clients in address '+sys.argv[1]+':'+sys.argv[2]+'...')
    c, addr = s.accept()
    print('Client connected')
    i=0
    with open('../files/redditSubmissions-sorted-date.csv') as fp:
        last_pos = fp.tell()
        for line in fp:
            if i>199:
                i=0
                time.sleep(60)
                print('New chunk sent')
            try:
                c.send(line)
                i=i+1
            except socket.error, e:
                print('Client disconnected\n')
                c, addr = s.accept()
                fp.seek(last_pos)
                print('Client connected')
                i=0
    c.close()
