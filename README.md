stream-tree-learning
====================

Distributed Decision Tree Prediction of Streaming Data using Spark.

Currently application does the following: Complete creation of the decision tree, without streaming.
Modules in DEVELOPMENT, they will be added to the main application as soon as they are tested and work properly.
Modules work and can be tested by shell.

TO RUN THE APPLICATION (Compiles automatically):
----------------------

Run 2 containers:
	sudo docker run -i -t aecc/stream-tree-learning /bin/bash

In one container run the streamer:
	scripts/start_streamer.sh <interface> <port> 

In other container run the application (it will compile automatically):
	sbt "run local[100] \<ip-streamer> \<port-streamer>"


