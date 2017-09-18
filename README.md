LS-CAT Image Server
===================

This project takes requests regarding diffraction images from the user
and returns the result.  The simplest request is for a jpeg version of
a diffraction image so that the user may view it on their browser.

Here are the main parts of this project

-# Communicating the request and its response

-# Running processes with the correct UID/GID

-# Performing the action


Starting witht he communications portion, we use [ZMQ](http://zeromq.org) as shown in the diagram below.


![Image Server Data Flow](../../isOverview.png)


Zero MQ
-------

-# User generates a request that is handeled by the @c is.js component
   of the LS-CAT Remote Access Server

-# Request is aggregated by @c is_proxy.  This allows for multiple
   instances of @c is.js (which we have) on possibly multiple web
   servers (which we do not have *yet*).  Note that the location of @c
   is_proxy is well know so that @c is.js and the Image Server Process
   Manager can connect to it.  This obviates the need for a more
   complex discovery mechanism.

-# The Image Server Process Manager receives the request and passes it
   on to a process running as the UID of the user and the GID of the
   ESAF that collected the data.

-# The process supervisor receives the request and passes it on to a
   worker thread.

-# The worker thread performs the work and passes the result back
   through the ZMQ pipes.

At any step an error message will be passed back instead of the result
when something goes wrong.


