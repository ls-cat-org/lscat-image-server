/** @file isMain.c
 *  @brief Runs the image server main loop
 *  @date 2017
 *  @copyright 2017 by Northwestern University. All rights reserved
 *  @author Keith Brister
 *
 *	gcc -Wall isEiger.c -o isEiger -lhdf5 -lhiredis -llz4 -ljansson -lpthread
 *
 *  Requests are made by placing a JSON request object (called
 *  isRequest here) onto a Redis list (ISREQUESTS).  When requests are
 *  taken from the list we need to verify the username associated with
 *  the request.  Unlike other web server requests this one will grant
 *  file system access as the user and, therefore, should be
 *  authentiated.
 *
 *  To authticate the user we look up an encrypted and signed message
 *  to us from the login server.  This message contains the user name
 *  as well as a copy of token used to identify this user session
 *  (called "pid" in isRequest).  If the token in the message matches
 *  that in isRequest then we go ahead and act upon the request.
 *
 *  We keep a list of processes running as our users and submit the
 *  isRequest job to the appropriate one.
 *
 *  This system should make it difficult for an attacker to forge an
 *  isRequest object to gain access to our system as someone other
 *  than themselves.  Note that we do not verify here that the request
 *  will attempt to act only upon data that the user has access to.
 *  For that we rely on the normal Unix file user and group access
 *  system.
 *
 */
#include "is.h"

/** Length of the zmq poll items
 **
 ** @todo As the name implies this should be the amount we increase
 ** the list.  Currently this list is a fixed size and this approach
 ** is probably a bad idea and should be reevaluated.
 **
 ** @todo We only correctly deal with the case were there is only one
 ** message part after the envelope messages.  Well, we do send on
 ** additional message parts but only in the (hopefully normal case)
 ** where there are no errors.  If there is an error then these extra
 ** messages parts go unread.  Probably this is a mistake but we do
 ** not yet have a use case for these extra parts so even if we did
 ** something clever we'd not be able to test it.  Hence this little
 ** warning message.
 */
#define N_ZPOLLITEMS_INC 128

/** Our entry point into this lovely world of zero mq and diffraction
 ** images.
 **
 ** @param argc Number of arguments on the command line
 **
 ** @param argv List of argument strings.  Currently we do not support
 **             any arguments.
 **
 */
int main(int argc, char **argv) {
  static const char *id = FILEID "main";

  // Make these static so the get initialized as NULL and our signal
  // handler works less badly
  //
  static struct sigaction sa;          // set up our signal handler
  static void *zctx;                   // Our ZMQ context.
  static void *router;                 // Router socket we recieve our commands on
  static void *err_dealer;             // Socket to read errors generated when a user process cannot be forked
  static void *err_rep;                // Socket to handle the above errors (We need the err sockets to keep all the code ZMQ protocol complaint)

  zmq_pollitem_t *zpollitems;   // list of sockets we need to service
  int n_zpollitems;             // number of said sockets

  zmq_msg_t zmsg;               // Move messages between various socket when we service them
  int nreceived;                // Bytes received in a ZMQ messages (or -1 on error)

  json_t  *isRequest;           // Request sent by the user.  Called "job" in other places in the code.
  redisContext *rc;             // connection to redis on the web server (for permission verifications)
  redisContext *rcLocal;        // connection to our local redis (for storage)
  json_t *isAuth;               // JSON object with our user's permission
  char *isAuth_str;             // string version of isAuth as received from redis
  char *isAuthSig;              // signature used to authenticate isAuth
  json_error_t jerr;            // error returned from most json_ routines
  redisReply *reply;            // reply received from redis
  redisReply *subreply;         // sub reply returned from redis
  char *pid;                    // Process ID for our user's instance
  int esaf;                     // esaf the user wants to access
  isProcessListType *pli;       // process descriptor for our user's session
  int err;                      // error code for routines that return ints
  int more;                     // indicates there are more message parts to read
  int i;                        // loop variable
  zmq_msg_t envelope_msgs[16];  // routing messages: likely there are no more than 2, 16 is way overkill but we'll break if there are more proxies than this between us and the user.
  int n_envelope_msgs;          // number of "envelope messages"
  int socket_option;            // used to set ZMQ socket options
  int dev_mode;                 // flag to use development sockets instead of production sockets

  //
  // Exit "elegantly" on ^C
  //
  void our_handler(int sig) {
    int i;
    if (router) {
      zmq_close(router);
    }
    
    if (err_dealer) {
      zmq_close(err_dealer);
    }

    if (err_rep) {
      zmq_close(err_rep);
    }

    for (i=2; i<n_zpollitems; i++) {
      zmq_close(zpollitems[i].socket);
    }

    if (zctx) {
      zmq_ctx_term(zctx);
    }
  }
  sa.sa_handler = our_handler;
  sa.sa_flags     = SA_SIGINFO | SA_RESTART;
  sigfillset(&sa.sa_mask);

  dev_mode = 0;
  if (strstr(argv[0],"dev")) {
    dev_mode = 1;
    isLogging_info("Developement Mode");
  };


  // Make sure we are the only "is" process running on this node.
  // Needed since we need to bind to a particular ipc socket AND if
  // our predecessor has died but left its ZMQ threads running we
  // don't want it to steal our messages (and do nothing with them)
  //
  isInit(dev_mode);

  isLogging_info("Welcome to the LS-CAT Image Server by Keith Brister ©2017-2018 by Northwestern University.  All rights reserved.\n");

  n_zpollitems = N_ZPOLLITEMS_INC;
  zpollitems = calloc(n_zpollitems, sizeof(*zpollitems));
  if (zpollitems == NULL) {
    isLogging_crit("%s: Out of memory (zpollites)\n", id);
    exit (-1);
  }

  isProcessListInit();

  //
  // setup redis
  //
  // Connection to web server to access permissions and login status
  //
  rc = redisConnect(REMOTE_SERVER_REDIS_ADDRESS, REMOTE_SERVER_REDIS_PORT);
  if (rc == NULL || rc->err) {
    if (rc) {
      isLogging_err("%s: Failed to connect to remote redis at %s: %s\n", id, REMOTE_SERVER_REDIS_ADDRESS, rc->errstr);
    } else {
      isLogging_err("%s: Failed to get redis remote context\n", id);
    }
    exit (-1);
  }

  //
  // Connection to our local data store
  //
  rcLocal = redisConnect("127.0.0.1", 6379);
  if (rcLocal == NULL || rcLocal->err) {
    if (rcLocal) {
      isLogging_err("%s: Failed to connect to local redis: %s\n", id, rcLocal->errstr);
    } else {
      isLogging_err("%s: Failed to get local redis context\n", id);
    }
    exit (-1);
  }

  // openssl initialization
  OpenSSL_add_all_digests();

  // Load the human readable error strings for libcrypto
  ERR_load_crypto_strings();

  // Load all digest and cipher algorithms
  OpenSSL_add_all_algorithms();

  //
  // Set up zeromq
  //
  zctx   = zmq_ctx_new();

  // Connection to the web server's is_proxy service that forwards user requests
  //
  router = zmq_socket(zctx, ZMQ_ROUTER);
  err = zmq_connect(router, dev_mode ? PUBLIC_DEV_DEALER : PUBLIC_DEALER);
  if (err == -1) {
    isLogging_err("%s: Failed to connect router to dealer %s: %s\n", id, dev_mode ? PUBLIC_DEV_DEALER : PUBLIC_DEALER, zmq_strerror(errno));
    exit (-1);
  }

  //
  // Disable the ZMQ receiver high water mark for the router
  //
  socket_option = 0;
  err = zmq_setsockopt(router, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set RCVHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  //
  // Disable the ZMQ sender high water mark for the router
  //
  socket_option = 0;
  err = zmq_setsockopt(router, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set SNDHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  // Connection to our interal error handler
  //
  // Just so we don't have to dig down any further into the internal
  // workings of ZMQ we'll handle errors by passing incoming (bad)
  // requests to err_dealer to which is then processed by err_rep.  We
  // could probably simplify this by faking the dealer/rep interchange
  // but then, why?  It's not really that complicated (right).
  //
  err_dealer = zmq_socket(zctx, ZMQ_DEALER);
  if (err_dealer == NULL) {
    isLogging_err("%s: Could not create dealer socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  //
  // Disable the error dealer receiver high water mark
  //
  socket_option = 0;
  err = zmq_setsockopt(err_dealer, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set RCVHWM for err_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  //
  // Disable the error dealer sender high water mark
  //
  socket_option = 0;
  err = zmq_setsockopt(err_dealer, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set SNDHWM for err_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  // Bind the err_dealer to its (and err_rep's) inproc socket
  //
  err        = zmq_bind(err_dealer, dev_mode ? ERR_DEV_REP : ERR_REP);
  if (err == -1) {
    isLogging_err("%s: Could not bind err_dealer to socket %s: %s\n", id, dev_mode ? ERR_DEV_REP : ERR_REP, zmq_strerror(errno));
    exit (-1);
  }

  // Create the error responder socket (that will interact with the err_dealer socket)
  //
  err_rep = zmq_socket(zctx, ZMQ_REP);
  if (err_rep == NULL) {
    isLogging_err("%s: Could not create err_rep socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  // Disable receiver high water mark for the error responder
  //
  socket_option = 0;
  err = zmq_setsockopt(err_rep, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set RCVHWM for err_rep: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  // Disable sender high water mark for the error responder
  //
  socket_option = 0;
  err = zmq_setsockopt(err_rep, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set SNDHWM for err_rep: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  // Connect the error responder to the bound error dealer
  //
  err     = zmq_connect(err_rep, dev_mode ? ERR_DEV_REP : ERR_REP);
  if (err == -1) {
    isLogging_err("%s: Could not connect err_rep to socket %s: %s\n", id, dev_mode ? ERR_DEV_REP : ERR_REP, zmq_strerror(errno));
    exit (-1);
  }

  // No envelope messages to close yet
  //
  n_envelope_msgs = 0;

  //
  // Here is our main loop
  //
  sigaction(SIGINT, &sa, NULL);
  while (1) {
    //
    // We are really just about servicing ZMQ sockets.  Similar to the
    // poll function for unix file descriptors we have a zmq poll
    // funciton for zmq sockets.  Cool.
    //
    // We'll just sit until one or more of our sockets has something
    // to say. Then we'll pass its message on to another socket (that
    // we may have to create a process to service).
    //

    // List of sockets to listen to
    zpollitems = isRemakeZMQPollItems(router, err_rep, err_dealer);
    n_zpollitems = isNProcesses() + 3;

    //
    // Wait for messages to appear
    //
    err = zmq_poll(zpollitems, n_zpollitems, -1);
    if (err == -1) {
      isLogging_err("%s: zmq_poll returned error (%d poll items): %s\n", id, n_zpollitems, zmq_strerror(errno));
      exit (-1);
    }
    
    //
    // zpollitems[0] is the router listening to is_proxy
    //
    // zpollitems[1] is the error responder passing messages back to the error dealer
    //
    // zpollitems[2] is the err_dealer sending packets to the error responder
    //
    // zpollitems[n] with n > 2 is the response from one of our processes destined for is_proxy
    //

    //
    // Error responder (err_rep).  We'll just echo the messages.
    //
    if (zpollitems[1].revents & ZMQ_POLLIN) {
      //
      // Echo messages sent to our error responder.
      //
      do {
        zmq_msg_init(&zmsg);
        zmq_msg_recv(&zmsg, err_rep, 0);
        more = zmq_msg_more(&zmsg);
        zmq_msg_send(&zmsg, err_rep, more ? ZMQ_SNDMORE : 0);
        zmq_close(&zmsg);
      } while(more);
    }

    //
    // Transfer all the child process chatter (as well as our error
    // messages) back to the is.js process.
    //
    //
    for (i=2; i<n_zpollitems; i++) {
      if (zpollitems[i].revents & ZMQ_POLLIN) {
        do {
          zmq_msg_init(&zmsg);
          zmq_msg_recv(&zmsg, zpollitems[i].socket, 0);
          more = zmq_msg_more(&zmsg);
          zmq_msg_send(&zmsg, router, more ? ZMQ_SNDMORE : 0);
          zmq_msg_close(&zmsg);
        } while (more);
      }
    }

    // [0] is the router.  We'll listen for new stuff and, perhaps if
    // we feel like it, pass the job request to the appropriate child,
    // starting it if necessary.
    //
    if (!(zpollitems[0].revents & ZMQ_POLLIN)) {
      //
      // Nothing incoming from is.js.  Just keep on truckin'.
      //
      continue;
    }

    //
    // @warning This is kind of a kludge.  We need to inspect the
    // request from the user to route the message to the correct
    // process but this requires us to use our knowledge of the ZMQ
    // message passing protocol.  Hence, we are vulnerable to changes
    // in the protocol in future releases.
    //
    // The "enevelope" referred to here consists of one message part
    // per proxy hop followed by a message of zero length.  After this
    // comes the payload destined for the worker but which we need to
    // look at here.
    //

    //
    // Find (and save) our envelope messages
    //
    for (i=0; i<sizeof(envelope_msgs)/sizeof(envelope_msgs[0]); i++) {
      zmq_msg_init(&envelope_msgs[i]);
      nreceived = zmq_msg_recv(&envelope_msgs[i], router, 0);
      if (nreceived == -1) {
        isLogging_err("%s: Error receiving envelope from public dealer: %s\n", id, zmq_strerror(errno));
        exit (-1);
      }
      more = zmq_msg_more(&envelope_msgs[i]);
      if (zmq_msg_size(&envelope_msgs[i]) == 0 || !more) {
        break;
      }
    }

    if (i == sizeof(envelope_msgs)/sizeof(envelope_msgs[i]) || !more) {
      isLogging_err("%s: Unexpected incoming message format. Too many router/dealers?\n", id);
      exit (-1);
    }

    n_envelope_msgs = i+1;

    //
    // Now we'll just take a peek at the message intended for the worker.
    //
    zmq_msg_init(&zmsg);
    nreceived = zmq_msg_recv(&zmsg, router, 0);
    if (nreceived == -1) {
      isLogging_err("%s: Error receiving message from public dealer: %s\n", id, zmq_strerror(errno));
      exit (-1);
    }

    //
    // If there are any more parts we'll recv & send them later
    //
    more = zmq_msg_more(&zmsg);

    //
    // Retrieve instructions from our client
    //
    isRequest = json_loadb(zmq_msg_data(&zmsg), zmq_msg_size(&zmsg), 0, &jerr);

    if (isRequest == NULL) {
      isLogging_err("%s: Failed to parse '%s': %s\n", id, (char *)zmq_msg_data(&zmsg), jerr.text);
      is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Failed to parse request: %s", id, jerr.text);
      continue;
    }

    pid = (char *)json_string_value(json_object_get(isRequest, "pid"));
    if (pid == NULL) {
      char *tmpstr;
      tmpstr = json_dumps(isRequest, JSON_SORT_KEYS | JSON_COMPACT | JSON_INDENT(0));
      isLogging_err("%s: isRequest without pid: %s\n", id, tmpstr);
      is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: request does not contain pid", id);
      free(tmpstr);
      json_decref(isRequest);
      continue;
    }

    esaf = json_integer_value(json_object_get(isRequest, "esaf"));

    isAuth = NULL;
    isLogging_info("%s: got pid %s  esaf %d\n", id, pid, esaf);
    pli = isFindProcess(pid, esaf);
    if (pli == NULL) {
      //
      // Here we've not yet authenticated this pid.
      //
      reply = redisCommand(rc, "HMGET %s isAuth isAuthSig", pid);
      if (reply == NULL) {
        isLogging_err("%s: Redis error (isAuth): %s\n", id, rc->errstr);
        exit(-1);
      }
    
      if (reply->type == REDIS_REPLY_ERROR) {
        isLogging_err("%s: Reids hmget isAuth produced an error: %s\n", id, reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_ARRAY) {
        if (reply->type == REDIS_REPLY_NIL) {
          isLogging_err("%s: Process %s is not active\n", id, pid);
          is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not active", id, pid);
        } else {
          isLogging_err("%s: Redis hmget isAuth isAuthSig did not return an array, got type %d\n", id, reply->type);
          is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (1)", id, pid);
        }

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }

      subreply = reply->element[0];
      if (subreply->type != REDIS_REPLY_STRING) {
        isLogging_err("%s: isAuth reply is not a string, got type %d\n", id, subreply->type);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (2)", id, pid);
        
        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      
      isAuth_str = subreply->str;

      subreply = reply->element[1];
      if (subreply->type != REDIS_REPLY_STRING) {
        isLogging_err("%s: isAuthSig reply is not a string, got type %d\n", id, subreply->type);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (3)", id, pid);
        
        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      
      isAuthSig = subreply->str;
        
      if (!verifyIsAuth( isAuth_str, isAuthSig)) {
        isLogging_err("%s: Bad isAuth signature for pid %s: isAuth_str: '%s'\n", id, pid, isAuth_str);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (4)", id, pid);

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      

      isAuth = json_loads(isAuth_str, 0, &jerr);
      if (isRequest == NULL) {
        isLogging_err("%s: Failed to parse '%s': %s\n", id, subreply->str, jerr.text);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (5)", id, pid);

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }
    
      {
        char *tmpsp;

        tmpsp=json_dumps(isAuth, JSON_INDENT(0)|JSON_COMPACT|JSON_SORT_KEYS);        
        isLogging_info("%s: isAuth: %s\n", id, tmpsp);
        free(tmpsp);
      }

      freeReplyObject(reply);

      if (strcmp(pid, json_string_value(json_object_get(isAuth, "pid"))) != 0) {
        isLogging_err("%s: pid from request does not match pid from isAuth: '%s' vs '%s'\n", id, pid, json_string_value(json_object_get(isAuth, "pid")));
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (6)", id, pid);

        json_decref(isRequest);
        json_decref(isAuth);
        continue;
      }
      if (!isEsafAllowed(isAuth, esaf)) {
        isLogging_err("%s: user %s is not permitted to access esaf %d\n", id, json_string_value(json_object_get(isAuth, "uid")), esaf);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized for esaf %d", id, pid, esaf);
        
        json_decref(isRequest);
        json_decref(isAuth);
        continue;
      }

      pli = isRun(zctx, rc, isAuth, esaf, dev_mode);
    } else {
      //
      // Here we've authenticated this pid (perhaps some time ago).  We
      // just need to verify that this pid is still active.
      //
      reply = redisCommand(rc, "EXISTS %s", pid);
      if (reply == NULL) {
        isLogging_err("%s: Redis error (exists pid): %s\n", id, rc->errstr);
        exit(-1);
      }
      
      if (reply->type == REDIS_REPLY_ERROR) {
        isLogging_err("%s: Reids exists pid produced an error: %s\n", id, reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_INTEGER) {
        isLogging_err("%s: Redis exists pid did not return an integer, got type %d\n", id, reply->type);
        exit (-1);
      }

      if (reply->integer != 1) {
        isLogging_err("%s: Process %s is no longer active\n", id, pid);
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (7)", id, pid);
        //
        // TODO: We need to periodically purge our process list of inactive processes
        //
        freeReplyObject(reply);
        json_decref(isRequest);
        continue;
      }
      freeReplyObject(reply);
    }

    json_decref(isRequest);

    //
    // Send our envelope messages to the supervisor (through parent_dealer)
    //
    for (i=0; i<n_envelope_msgs; i++) {
      zmq_msg_send(&envelope_msgs[i], pli->parent_dealer, ZMQ_SNDMORE);
      zmq_msg_close(&envelope_msgs[i]);
    }

    zmq_msg_send(&zmsg, pli->parent_dealer, more ? ZMQ_SNDMORE : 0);
    zmq_msg_close(&zmsg);

    //
    // If there just happens to be some more message parts to pass on
    // this is where that magic happens.
    //
    while (more) {
      zmq_msg_init(&zmsg);
      zmq_msg_recv(&zmsg, router, 0);
      more = zmq_msg_more(&zmsg);
      zmq_msg_send(&zmsg, pli->parent_dealer, more ? ZMQ_SNDMORE : 0);
      zmq_msg_close(&zmsg);
    }

    if (isAuth != NULL) {
      json_decref(isAuth);
      isAuth = NULL;
    }
  }

  return 0;
}
