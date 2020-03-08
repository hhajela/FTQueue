1. To run the program, first run the create server script by executing command "sh createServers.sh"
2. This will invoke the command python3 server.py along with the server node number argument to bring up all the servers, init their socket connections and set them to listening.
3. After this run the testing script by executing command "python3 testing.py"
4. This will do a few different queue operations and verify that the operations are being performed correctly and that the state is being replicated across the nodes correctly.

What Works:-

1. Verified that queue operations exposed (qCreate, qDestroy, qId, qPop, qPush, qDestroy, qTop, qSize) are working correctly.
2. Verified that state is being replicated correctly
3. Verified that the message order is being followed correctly by all nodes and that the sequencer role is rotating correctly.

Possible sources of errors:-

1. Bounds and exceptional cases handling on queue operations such as popping from an empty queue and deleting a non existent queue have not been verified thoroughly.
