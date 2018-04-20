/*! @file isWorker.c
 *  @copyright 2017 by Northwestern University All Rights Reserved
 *  @author Keith Brister
 *  @brief Routines to parse instructions from the user and fire off the appropriate actions.
 */
#include "is.h"
static int running = 0;

/** handle signals by lowering the running flag.
 ** 
 ** @param sig the signal number
 **
 ** @param si  siginfo struction
 **
 ** @param dummy unused user data
 */
void signal_action(int sig, siginfo_t *si, void *dummy) {
  if (sig == SIGTERM || sig == SIGINT) {
    running = 0;
  }
}

/** Dispatch jobs sent from the supervisor via zmq
 **
 ** @param voidp  opaque pointer to our worker context
 */
void *isWorker(void *voidp) {
  static const char *id = FILEID "isWorker";
  isThreadContextType tc;
  isWorkerContext_t *wctx;
  json_t *job;
  json_error_t jerr;
  char *jobstr;
  const char *job_type;
  char dealer_endpoint[128];
  zmq_msg_t zmsg;
  int err;
  int socket_option;

  // make gcc happy
  wctx = voidp;

  tc.rep = zmq_socket(wctx->zctx, ZMQ_REP);
  if (tc.rep == NULL) {
    isLogging_err("%s: failed to create zmq socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(tc.rep, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set RCVHWM for rc.rep: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(tc.rep, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set SNDHWM for rc.rep: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "inproc://#%s", wctx->key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;
  
  err = zmq_connect(tc.rep, dealer_endpoint);
  if (err == -1) {
    isLogging_err("%s: Failed to connect to dealer endpoint %s: %s\n", id, dealer_endpoint, zmq_strerror(errno));
    exit (-1);
  }

  //
  // setup redis
  //
  tc.rc = redisConnect("127.0.0.1", 6379);
  if (tc.rc == NULL || tc.rc->err) {
    if (tc.rc != NULL) {
      isLogging_err("%s: Failed to connect to redis: %s\n", id, tc.rc->errstr);
    } else {
      isLogging_err("%s: Failed to get redis context\n", id);
    }
    fflush(stderr);
    exit (-1);
  }

  while (1) {
    //
    // Wait for something to do
    //
    zmq_msg_init(&zmsg);
    err = zmq_msg_recv(&zmsg, tc.rep, 0);
    if (err == -1) {
      isLogging_err("%s: problem receiving message: %s\n", id, zmq_strerror(errno));
      zmq_msg_close(&zmsg);
      if (errno == EFSM) {
        //
        // Trick to get socket back to the right state
        //
        // TODO: How, exactly did we get into the wrong state to begin
        // with?  Do we have an error somewhere that does not call
        // is_zmq_error_reply?
        //
        is_zmq_error_reply(NULL, 0, tc.rep, "%s: Socket in wrong state", id);
        continue;
      }
    }

    pthread_mutex_lock(&wctx->metaMutex);
    job = json_loadb(zmq_msg_data(&zmsg), zmq_msg_size(&zmsg), 0, &jerr);
    pthread_mutex_unlock(&wctx->metaMutex);


    zmq_msg_close(&zmsg);

    if (job == NULL) {
      isLogging_err("%s: Failed to parse request: %s\n", id, jerr.text);
      is_zmq_error_reply(NULL, 0, tc.rep, "%s: Could not parse request: %s", id, jerr.text);
      continue;
    }

    pthread_mutex_lock(&wctx->metaMutex);
    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);
    job_type = json_string_value(json_object_get(job, "type"));
    pthread_mutex_unlock(&wctx->metaMutex);

    if (job_type == NULL) {
      isLogging_err("%s: No type parameter in job %s\n", id, jobstr);
      is_zmq_error_reply(NULL, 0, tc.rep, "%s: No type parameter in job %s", id, jobstr);
    } else {
      // Cheapo command parser.  Probably the best way to go considering
      // the small number of commands we'll likely have to service.
      if (strcasecmp("jpeg", job_type) == 0) {
        isJpeg(wctx, &tc, job);
      } else if (strcasecmp("index", job_type) == 0) {
        isIndex(wctx, &tc, job);
      } else if (strcasecmp("spots", job_type) == 0) {
        isSpots(wctx, &tc, job);
      } else {
        isLogging_err("%s: Unknown job type '%s' in job '%s'\n", id, job_type, jobstr);
        is_zmq_error_reply(NULL, 0, tc.rep, "%s: Unknown job type '%s' in job '%s'", id, job_type, jobstr);
      }
    }
    free(jobstr);
    pthread_mutex_lock(&wctx->metaMutex);
    json_decref(job);
    pthread_mutex_unlock(&wctx->metaMutex);
  }
  return NULL;
}

/** Dispatch workers and pass to them the jobs we receive.
 ** 
 ** @param key  Unique identifer for this user in this ESAF group
 */
void isSupervisor(const char *key) {
  static const char *id = FILEID "isSupervisor";
  static struct sigaction si;
  //
  // In child process running as user in home directory
  //
  zmq_pollitem_t zpollitems[2];
  isWorkerContext_t *wctx;
  int i;
  int err;
  pthread_t threads[N_WORKER_THREADS];
  zmq_msg_t zmsg;
  int more;

  mtrace();

  si.sa_sigaction = signal_action;
  sigemptyset(&si.sa_mask);
  sigaction( SIGTERM, &si, NULL);
  sigaction( SIGINT,  &si, NULL);

  running = 1;
  wctx = isDataInit(key);

  // Start up some workers
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_create(&(threads[i]), NULL, isWorker, wctx);

    if (err != 0) {
      isLogging_err("%s: Could not start worker for %s because %s\n",
              id, key, err==EAGAIN ? "Insufficient resources" : (err==EINVAL ? "Bad attributes" : (err==EPERM ? "No permission" : "Unknown Reasons")));
      return;
    }
  }

  zpollitems[0].socket = wctx->dealer;
  zpollitems[0].events = ZMQ_POLLIN;

  zpollitems[1].socket = wctx->router;;
  zpollitems[1].events = ZMQ_POLLIN;

  while(running) {
    err = zmq_poll(zpollitems, 2, -1);
    if (err == -1) {
      if (errno == EINTR) {
        break;
      }
      isLogging_err("%s: zmq_poll error: %s\n", id, zmq_strerror(errno));
      exit (-1);
    }

    if (zpollitems[0].revents & ZMQ_POLLIN) {
      while(1) {
        zmq_msg_init(&zmsg);
        zmq_msg_recv(&zmsg, wctx->dealer, 0);
        more = zmq_msg_more(&zmsg);
        zmq_msg_send(&zmsg, wctx->router, more ? ZMQ_SNDMORE : 0);
        zmq_msg_close(&zmsg);
        if (!more) {
          break;
        }
      }
    }

    if (zpollitems[1].revents & ZMQ_POLLIN) {
      while(1) {
        zmq_msg_init(&zmsg);
        zmq_msg_recv(&zmsg, wctx->router, 0);
        more = zmq_msg_more(&zmsg);
        zmq_msg_send(&zmsg, wctx->dealer, more ? ZMQ_SNDMORE : 0);
        zmq_msg_close(&zmsg);
        if (!more) {
          break;
        }
      }
    }
  }

  // TODO: do we need to send a signal to the threads (pthreads_kill)?

  // Wait for the workers to stop
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_join(threads[i], NULL);
    switch(err) {
    case EDEADLK:
      isLogging_err("%s: deadlock detected on join for thread %d\n", id, i);
      break;
    case EINVAL:
      isLogging_err("%s: thread %d is unjoinable\n", id, i);
      break;
    case ESRCH:
      isLogging_err("%s: thread %d could not be found\n", id, i);
      break;
    }
  }

  // free up the image buffers
  isDataDestroy(wctx);
  return;
}
