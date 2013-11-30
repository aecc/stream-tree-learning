stream-tree-learning
====================

Distributed Decision Tree Prediction of Streaming Data using Spark

CHANGE LOG
----------

It's now split in modules to work in a collaborative way


TO RUN THE APPLICATION:
----------------------

Run 2 containers:
	sudo docker run -i -t aecc/stream-tree-learning:[Version] /bin/bash

In one container run the streamer:
	scripts/start_streamer.sh <port> 

In other container run the application (it will compile automatically):
	sbt "run local[2] <ip-streamer> <port-streamer>"


