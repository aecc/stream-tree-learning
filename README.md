stream-tree-learning
====================

Distributed Decision Tree Prediction of Streaming Data using Spark.

Currently application does the following: Complete creation of the decision tree, without streaming.

function Evaluate.predictEntry has a BUG that hasn't been fixed! not working completely.

TO RUN THE APPLICATION (Compiles automatically):
----------------------

Run 2 containers:
	sudo docker run -i -t aecc/stream-tree-learning /bin/bash

In one container run the streamer:
	scripts/start_streamer.sh <interface> <port> 

In other container run the application (it will compile automatically):
	sbt "run local[100] \<ip-streamer> \<port-streamer>"


