
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

int main(int argc, char **argv) {
  json_t  *isRequest;
  redisContext *rc;
  json_t *isAuth_obj;
  json_error_t jerr;
  redisReply *reply;
  redisReply *subreply;
  const char *gpg_version;
  gpgme_ctx_t gpg_ctx;
  gpgme_error_t gpg_err;
  char *pid;

  isProcessListInit();

  //
  // setup redis
  //
  rc = redisConnect("10.1.253.10", 6379);
  if (rc == NULL || rc->err) {
    if (rc) {
      fprintf(stderr, "Failed to connect to redis: %s\n", rc->errstr);
    } else {
      fprintf(stderr, "Failed to get redis context\n");
    }
    exit (-1);
  }

  //
  // Setup gpg
  //
  gpg_version = gpgme_check_version(NULL);
  fprintf(stderr, "Using gpg version %s\n", gpg_version);

  gpg_err = gpgme_new(&gpg_ctx);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "gpg error creating context: %s\n", gpgme_strerror(gpg_err));
    exit (-1);
  }

  gpg_err = gpgme_set_protocol(gpg_ctx, GPGME_PROTOCOL_OpenPGP);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "Could not set gpg protocol: %s\n", gpgme_strerror(gpg_err));
    exit (-1);
  }

  gpg_err = gpgme_ctx_set_engine_info(gpg_ctx, GPGME_PROTOCOL_OpenPGP, "/usr/bin/gpg", "/pf/people/edu/northwestern/k-brister/.gnupg");
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "Could not set gpg engine info: %s\n", gpgme_strerror(gpg_err));
    exit (-1);
  }

  //
  // Here is our main loop
  //
  while (1) {
    //
    // Blocking request with no timeout.  We should be sitting here
    // patiently waiting for something to do.
    //
    // TODO: Consider implimenting this as asynchronous requests to
    // speed up sending the isRequests to processes that actually do
    // work.  This might be needed when the image server gets very
    // busy.
    //
    reply = redisCommand(rc, "BRPOP ISREQUESTS 0");

    //
    // Retrieve and parse our instructions
    //

    if (reply == NULL) {
      fprintf(stderr, "Redis error: %s\n", rc->errstr);
      exit (-1);
    }

    if (reply->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "Redis brpop command produced an error: %s\n", reply->str);
      exit (-1);
    }
  
    if (reply->type != REDIS_REPLY_ARRAY) {
      fprintf(stderr, "Redis brpop did not return an array, got type %d\n", reply->type);
      exit(-1);
    }
    
    if (reply->elements != 2) {
      fprintf(stderr, "Redis bulk reply length should have been 2 but instead was %d\n", (int)reply->elements);
      exit(-1);
    }
    subreply = reply->element[1];
    if (subreply->type != REDIS_REPLY_STRING) {
      fprintf(stderr, "Redis brpop did not return a string, got type %d\n", subreply->type);
      exit (-1);
    }

    isRequest = json_loads(subreply->str, 0, &jerr);
    if (isRequest == NULL) {
      fprintf(stderr, "Failed to parse '%s': %s\n", subreply->str, jerr.text);
      continue;
    }
    freeReplyObject(reply);

    pid = (char *)json_string_value(json_object_get(isRequest, "pid"));
    if (pid == NULL) {
      fprintf(stderr, "isRequest without pid\n");

      json_decref(isRequest);
      continue;
    }

    isAuth_obj = NULL;
    if (!isHasProcess(pid)) {
      //
      // Here we've not yet authenticated this pid.
      //
      reply = redisCommand(rc, "HGET %s isAuth", pid);
      if (reply == NULL) {
        fprintf(stderr, "Redis error (isAuth): %s\n", rc->errstr);
        exit(-1);
      }
    
      if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "Reids hget isAuth produced an error: %s\n", reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_STRING) {
        fprintf(stderr, "Redis hget isAuth did not return a string, got type %d\n", reply->type);

        json_decref(isRequest);
        freeReplyObject(reply);
        continue;
      }

      //
      // TODO: Once the LS CA is up an running (and we've produced and
      // installed the required certs) go ahead and verify that the
      // message signature is tracable to LS CA.
      //
      isAuth_obj = decryptIsAuth( gpg_ctx, reply->str);
      fprintf(stdout, "isAuth_obj:\n");
      json_dumpf(isAuth_obj, stdout, JSON_INDENT(0)|JSON_COMPACT|JSON_SORT_KEYS);
      fprintf(stdout, "\n");
      freeReplyObject(reply);

      if (strcmp(pid, json_string_value(json_object_get(isAuth_obj, "pid"))) != 0) {
        fprintf(stderr, "pid from request does not match pid from isAuth: '%s' vs '%s'\n", pid, json_string_value(json_object_get(isAuth_obj, "pid")));

        json_decref(isRequest);
        json_decref(isAuth_obj);
        continue;
      }
    } else {
      //
      // Here we've authenticated this pid (perhaps some time ago).  We
      // just need to verify that this pid is still active.
      //
      reply = redisCommand(rc, "EXISTS %s", pid);
      if (reply == NULL) {
        fprintf(stderr, "Redis error (exists pid): %s\n", rc->errstr);
        exit(-1);
      }
      
      if (reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "Reids exists pid produced an error: %s\n", reply->str);
        exit(-1);
      }

      if (reply->type != REDIS_REPLY_INTEGER) {
        fprintf(stderr, "Redis exists pid did not return an integer, got type %d\n", reply->type);
        exit (-1);
      }

      if (reply->integer != 1) {
        isProcessDoNotCall(pid);
        fprintf(stderr, "Process %s is no longer active\n", pid);

        freeReplyObject(reply);
        json_decref(isRequest);
        continue;
      }
      freeReplyObject(reply);
    }

    isRun(isAuth_obj, isRequest);

    json_decref(isRequest);
    if (isAuth_obj != NULL) {
      json_decref(isAuth_obj);
      isAuth_obj = NULL;
    }
  }

  return 0;
}
