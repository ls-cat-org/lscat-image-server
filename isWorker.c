#include "is.h"
static int running = 0;

void signal_action(int sig, siginfo_t *si, void *dummy) {
  if (sig == SIGTERM || sig == SIGINT) {
    running = 0;
  }
}

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

  // make gcc happy
  wctx = voidp;

  tc.rep = zmq_socket(wctx->zctx, ZMQ_REP);
  if (tc.rep == NULL) {
    fprintf(stderr, "%s: failed to create zmq socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "inproc://#%s", wctx->key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;
  
  err = zmq_connect(tc.rep, dealer_endpoint);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to connect to dealer endpoint %s: %s\n", id, dealer_endpoint, zmq_strerror(errno));
    exit (-1);
  }

  //
  // setup redis
  //
  tc.rc = redisConnect("127.0.0.1", 6379);
  fflush(stdout);
  if (tc.rc == NULL || tc.rc->err) {
    if (tc.rc != NULL) {
      fprintf(stderr, "%s: Failed to connect to redis: %s\n", id, tc.rc->errstr);
    } else {
      fprintf(stderr, "%s: Failed to get redis context\n", id);
    }
    fflush(stderr);
    exit (-1);
  }

  while (1) {
    //
    // Wait for something to do
    //
    zmq_msg_init(&zmsg);
    do {
      errno = 0;
      err = zmq_msg_recv(&zmsg, tc.rep, 0);
    } while(err == -1 && errno == EINTR);

    job = json_loads(zmq_msg_data(&zmsg), 0, &jerr);
    zmq_msg_close(&zmsg);

    if (job == NULL) {
      is_zmq_error_reply(NULL, 0, tc.rep, "%s: Could not parse request: %s", id, jerr.text);
      fprintf(stderr, "%s: Failed to parse request: %s\n", id, jerr.text);
      continue;
    }

    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);

    job_type = json_string_value(json_object_get(job, "type"));
    if (job_type == NULL) {
      is_zmq_error_reply(NULL, 0, tc.rep, "%s: No type parameter in job %s", id, jobstr);
      fprintf(stderr, "%s: No type parameter in job %s\n", id, jobstr);
    } else {
      // Cheapo command parser.  Probably the best way to go considering
      // the small number of commands we'll likely have to service.
      if (strcasecmp("jpeg", job_type) == 0) {
        isJpeg(wctx, &tc, job);
      } else {
        is_zmq_error_reply(NULL, 0, tc.rep, "%s: Unknown job type '%s' in job '%s'", id, job_type, jobstr);
        fprintf(stderr, "%s: Unknown job type '%s' in job '%s'\n", id, job_type, jobstr);
      }
    }
    free(jobstr);
    json_decref(job);
  }
  return NULL;
}

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
      fprintf(stderr, "%s: Could not start worker for %s because %s\n",
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
      fprintf(stderr, "%s: zmq_poll error: %s\n", id, zmq_strerror(errno));
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
      fprintf(stderr, "%s: deadlock detected on join for thread %d\n", id, i);
      break;
    case EINVAL:
      fprintf(stderr, "%s: thread %d is unjoinable\n", id, i);
      break;
    case ESRCH:
      fprintf(stderr, "%s: thread %d could not be found\n", id, i);
      break;
    }
  }

  // free up the image buffers
  isDataDestroy(wctx);
  return;
}
