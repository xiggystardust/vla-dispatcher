# vla-dispatcher Readme file.


----------------------
- On the server end: -
----------------------

Both the below should be run simultaneously, pointed toward the same
incoming.cmd file.


> vla_dispatcher/vla_server/fcn_server.py ()

This starts the server, which reads hosts in hosts.cfg, watches for
the incoming.cmd file, and then sends any incoming.cmd commands to the
hosts. Note, "hosts" are actually receiving clients.


> vla_dispatcher/dispatcher.py

This watches the VLA's MCAF stream, specifically the obsdoc file. It
then queues dispatch commands in incoming.cmd for the server to pick
up and send.



----------------------
- On the client end: -
----------------------

The directory client_tools/ contains two examples (they're pretty much
the same---just pick one to look at) of the code that should be run by
the client. This code sets up listening socket on a static IP and port
specified on the command line. This IP and port should be the same one
that VLA is planning to send packets to (as listed in the
aforementioned hosts.cfg file). The script then waits for packets,
unpacks the packets and interprets them depending on the event
type. It is up to the client-end user to then add code to decide what
to do with each event notification. Currently, sarahhome-test.py
simply prints out that a dispatch was received, and reports the values
that were received to the logger.
