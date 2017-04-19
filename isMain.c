
/** @file isMain.c
 *  @brief Runs the image server main loop
 *  @date 2017
 *  @copyright 2017 by Northwestern University
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
 */
#include "is.h"

#define N_ZPOLLITEMS_INC 128

int main(int argc, char **argv) {
  static const char *id = FILEID "main";

  zmq_pollitem_t *zpollitems;
  int n_zpollitems;

  void *zctx;
  void *router;
  void *err_dealer;
  void *err_rep;
  zmq_msg_t zmsg;
  int nreceived;

  json_t  *isRequest;
  redisContext *rc;
  redisContext *rcLocal;
  json_t *isAuth;
  char *isAuth_str;
  char *isAuthSig;
  json_error_t jerr;
  redisReply *reply;
  redisReply *subreply;
  char *pid;
  int esaf;
  isProcessListType *pli;
  int err;
  int more;
  int i;
  zmq_msg_t envelope_msgs[16];  // routing messages: likely there are no more than 2
  int n_envelope_msgs;

  n_zpollitems = N_ZPOLLITEMS_INC;
  zpollitems = calloc(n_zpollitems, sizeof(*zpollitems));
  if (zpollitems == NULL) {
    fprintf(stderr, "%s: Out of memory (zpollites)\n", id);
    exit (-1);
  }

  isProcessListInit();

  //
  // setup redis
  //
  rc = redisConnect("10.1.253.10", 6379);
  if (rc == NULL || rc->err) {
    if (rc) {
      fprintf(stderr, "%s: Failed to connect to redis: %s\n", id, rc->errstr);
    } else {
      fprintf(stderr, "%s: Failed to get redis context\n", id);
    }
    exit (-1);
  }

  rcLocal = redisConnect("127.0.0.1", 6379);
  if (rcLocal == NULL || rcLocal->err) {
    if (rcLocal) {
      fprintf(stderr, "%s: Failed to connect to redis: %s\n", id, rcLocal->errstr);
    } else {
      fprintf(stderr, "%s: Failed to get redis context\n", id);
    }
    exit (-1);
  }

  // openssl initialization
  OpenSSL_add_all_digests();
  /* Load the human readable error strings for libcrypto */
  ERR_load_crypto_strings();

  /* Load all digest and cipher algorithms */
  OpenSSL_add_all_algorithms();

  //
  // Set up zeromq
  //
  zctx   = zmq_ctx_new();

  // Connection to the web server that handles user requests
  router = zmq_socket(zctx, ZMQ_ROUTER);
  err = zmq_connect(router, PUBLIC_DEALER);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to connect router to dealer %s: %s\n", id, PUBLIC_DEALER, zmq_strerror(errno));
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
  err        = zmq_bind(err_dealer, ERR_REP);
  if (err == -1) {
    fprintf(stderr, "%s: Could not bind err_dealer to socket %s: %s\n", id, ERR_REP, zmq_strerror(errno));
    exit (-1);
  }

  err_rep = zmq_socket(zctx, ZMQ_REP);
  err     = zmq_connect(err_rep, ERR_REP);
  if (err == -1) {
    fprintf(stderr, "%s: Could not connect err_rep to socket %s: %s\n", id, ERR_REP, zmq_strerror(errno));
    exit (-1);
  }

  zpollitems = isRemakeZMQPollItems(router, err_dealer, err_rep);
  n_zpollitems = isNProcesses() + 3;

  fprintf(stdout, "%s: n_zpollitems=%d\n", id, n_zpollitems);

  //
  // Here is our main loop
  //
  while (1) {
    zpollitems = isGetZMQPollItems();
    n_zpollitems = isNProcesses() + 3;

    err = zmq_poll(zpollitems, n_zpollitems, -1);
    if (err == -1) {
      fprintf(stderr, "%s: zmq_poll returned error (%d poll items): %s\n", id, n_zpollitems, zmq_strerror(errno));
      exit (-1);
    }
    
    //
    // This is the error responder (err_rep).  Copy messages to the
    // err_dealer.
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
    // messages) back to the is.js
    //
    //
    for (i=2; i<n_zpollitems; i++) {
      if (zpollitems[i].revents & ZMQ_POLLIN) {
        do {
          zmq_msg_init(&zmsg);
          zmq_msg_recv(&zmsg, zpollitems[i].socket, 0);
          fprintf(stdout, "%s: recv %d bytes from dealer %d\n", id, (int)zmq_msg_size(&zmsg), i);
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
      // Nothing incoming from is.js.  Just keep on truckin.
      //
      continue;
    }

    for (i=0; i<sizeof(envelope_msgs)/sizeof(envelope_msgs[0]); i++) {
      zmq_msg_init(&envelope_msgs[i]);
      nreceived = zmq_msg_recv(&envelope_msgs[i], router, 0);
      if (nreceived == -1) {
        fprintf(stderr, "%s: Error receiving envelope from public dealer: %s\n", id, zmq_strerror(errno));
        exit (-1);
      }
      more = zmq_msg_more(&envelope_msgs[i]);
      if (zmq_msg_size(&envelope_msgs[i]) == 0 || !more) {
        break;
      }
    }

    if (i == sizeof(envelope_msgs)/sizeof(envelope_msgs[i]) || !more) {
      fprintf(stderr, "%s: Unexpected incoming message format. Too many router/dealers?\n", id);
      exit (-1);
    }

    n_envelope_msgs = i+1;

    zmq_msg_init(&zmsg);
    nreceived = zmq_msg_recv(&zmsg, router, 0);
    if (nreceived == -1) {
      fprintf(stderr, "%s: Error receiving message from public dealer: %s\n", id, zmq_strerror(errno));
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
      is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Failed to parse request: %s", id, jerr.text);
      fprintf(stderr, "%s: Failed to parse '%s': %s\n", id, (char *)zmq_msg_data(&zmsg), jerr.text);
      continue;
    }

    pid = (char *)json_string_value(json_object_get(isRequest, "pid"));
    if (pid == NULL) {
      char *tmpstr;
      tmpstr = json_dumps(isRequest, JSON_SORT_KEYS | JSON_COMPACT | JSON_INDENT(0));
      is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: request does not contain pid", id);
      fprintf(stderr, "%s: isRequest without pid: %s\n", id, tmpstr);
      free(tmpstr);

      json_decref(isRequest);
      continue;
    }

    esaf = json_integer_value(json_object_get(isRequest, "esaf"));

    isAuth = NULL;
    pli = isFindProcess(pid, esaf);
    if (pli == NULL) {
      //
      // Here we've not yet authenticated this pid.
      //
      reply = redisCommand(rc, "HMGET %s isAuth isAuthSig", pid);
      if (reply == NULL) {
        fprintf(stderr, "%s: Redis error (isAuth): %s\n", id, rc->errstr);
        exit(-1);
      }
    
      if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "%s: Reids hmget isAuth produced an error: %s\n", id, reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_ARRAY) {
        if (subreply->type == REDIS_REPLY_NIL) {
          is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not active", id, pid);
          fprintf(stderr, "%s: Process %s is not active\n", id, pid);
        } else {
          is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (1)", id, pid);
          fprintf(stderr, "%s: Redis hmget isAuth isAuthSig did not return an array, got type %d\n", id, reply->type);
        }

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }

      subreply = reply->element[0];
      if (subreply->type != REDIS_REPLY_STRING) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (2)", id, pid);
        fprintf(stderr, "%s: isAuth reply is not a string, got type %d\n", id, subreply->type);
        
        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      
      isAuth_str = subreply->str;

      subreply = reply->element[1];
      if (subreply->type != REDIS_REPLY_STRING) {
          is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (3)", id, pid);
        fprintf(stderr, "%s: isAuthSig reply is not a string, got type %d\n", id, subreply->type);
        
        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      
      isAuthSig = subreply->str;
        
      if (!verifyIsAuth( isAuth_str, isAuthSig)) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (4)", id, pid);
        fprintf(stderr, "%s: Bad isAuth signature for pid %s: isAuth_str: '%s'\n", id, pid, isAuth_str);

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }      

      isAuth = json_loads(isAuth_str, 0, &jerr);
      if (isRequest == NULL) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (5)", id, pid);
        fprintf(stderr, "%s: Failed to parse '%s': %s\n", id, subreply->str, jerr.text);

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }
    
      fprintf(stdout, "%s: isAuth:\n", id);
      json_dumpf(isAuth, stdout, JSON_INDENT(0)|JSON_COMPACT|JSON_SORT_KEYS);
      fprintf(stdout, "\n");
      freeReplyObject(reply);

      if (strcmp(pid, json_string_value(json_object_get(isAuth, "pid"))) != 0) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, err_dealer, "%s: Process %s is not authorized (6)", id, pid);
        fprintf(stderr, "%s: pid from request does not match pid from isAuth: '%s' vs '%s'\n", id, pid, json_string_value(json_object_get(isAuth, "pid")));

        json_decref(isRequest);
        json_decref(isAuth);
        continue;
      }
      if (!isEsafAllowed(isAuth, esaf)) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized for esaf %d", id, pid, esaf);
        fprintf(stderr, "%s: user %s is not permitted to access esaf %d\n", id, json_string_value(json_object_get(isAuth, "uid")), esaf);
        
        json_decref(isRequest);
        json_decref(isAuth);
        continue;
      }

      pli = isRun(zctx, rc, isAuth, esaf);
    } else {
      //
      // Here we've authenticated this pid (perhaps some time ago).  We
      // just need to verify that this pid is still active.
      //
      reply = redisCommand(rc, "EXISTS %s", pid);
      if (reply == NULL) {
        fprintf(stderr, "%s: Redis error (exists pid): %s\n", id, rc->errstr);
        exit(-1);
      }
      
      if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "%s: Reids exists pid produced an error: %s\n", id, reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_INTEGER) {
        fprintf(stderr, "%s: Redis exists pid did not return an integer, got type %d\n", id, reply->type);
        exit (-1);
      }

      if (reply->integer != 1) {
        is_zmq_error_reply(envelope_msgs, n_envelope_msgs, router, "%s: Process %s is not authorized (7)", id, pid);
        fprintf(stderr, "%s: Process %s is no longer active\n", id, pid);
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

    for (i=0; i<n_envelope_msgs; i++) {
      zmq_msg_send(&envelope_msgs[i], pli->parent_dealer, ZMQ_SNDMORE);
      zmq_msg_close(&envelope_msgs[i]);
    }
    zmq_msg_send(&zmsg, pli->parent_dealer, more ? ZMQ_SNDMORE : 0);
    zmq_msg_close(&zmsg);

    //
    // If there just happens to be some more message parts to pass on
    // to the thread that's actually doing some work this is where the
    // magic happens.
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
