# vla-dispatcher Readme file.

The VLA Dispatcher is a framework to read the Jansky Very Large Array
meta-data broadcast (position, observing set-up, etc), identify
relevant events, and broadcast information about those events to
approved clients running the client-end software.

The client-end software, described below, can be easily adapted to new
observatories. Currently this code is primarily used to enable
automated, coordinated observations of Fast Radio Bursts and other
transients between VLA and other interested (optical, radio, etc)
observatories.

For info contact S. Burke-Spolaor (sarahbspolaor@gmail.com)

Server comms base adapted from code by J. D. Dowell

----------------------
- On the server end: -
----------------------

Both the below should be run simultaneously, pointed toward the same
incoming.cmd file (or just run from the same directory with no
incoming.cmd specified on the command line).


> vla_dispatcher/vla_server/fcn_server.py

This starts the dispatching server, which reads hosts in hosts.cfg,
watches for the incoming.cmd file, and then sends any incoming.cmd
commands to the hosts. Note, "hosts" are actually receiving clients.


> vla_dispatcher/dispatcher.py --dispatch

This watches the VLA's MCAF stream, specifically the obsdoc file. It
then queues dispatch commands in incoming.cmd for the server to pick
up and send.



----------------------
- On the client end: -
----------------------

> client_tools/client_software.py

This gives an example of the code that should be run by the
client. This code sets up listening socket on a static IP and port
specified on the command line. This IP and port should be the same one
that VLA is planning to send packets to (as listed in the
aforementioned hosts.cfg file---that is, please notify S.Burke-Spolaor
of your IP and port if you want to receive communications). The script
then waits for packets, unpacks the packets and interprets them
depending on the event type. It is up to the client-end user to then
add code to decide what to do with each event notification. Currently,
client_software.py simply prints out that a dispatch was received, and
reports the values that were received. Within the code look for the
string "!!!!!!!!!!!!!"; in that location there is some descriptive
documentation and a hook for where client-end decisions should be
made.
