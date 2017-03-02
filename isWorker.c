#include "is.h"

void isJobQueuePush( isProcessListType *p, json_t *job) {
  // Called from parent
  //
  json_incref(job);

  pthread_mutex_lock(&p->job_mutex);
  p->job_queue[(p->job_on++) % IS_JOB_QUEUE_LENGTH] = json_dumps(job, JSON_COMPACT | JSON_INDENT(0) | JSON_SORT_KEYS);

  pthread_cond_signal(&p->job_cond);
  pthread_mutex_unlock(&p->job_mutex);
}

json_t *isJobQueuePop(isProcessListType *p) {
  json_t *rtn;
  json_error_t jerr;
  char *job;




  return rtn;
}


void *isWorker(void *p) {
  redisReply *reply;
  redisReply *subreply;
  json_t *job;
  char *jobstr;

  //
  // setup redis
  //
  rc = redisConnect("localhost", 6379);
  if (rc == NULL || rc->err) {
    if (rc) {
      fprintf(stderr, "Failed to connect to redis: %s\n", rc->errstr);
    } else {
      fprintf(stderr, "Failed to get redis context\n");
    }
    exit (-1);
  }

  fprintf(stderr, "isWorker: **** Here I am\n");

  while (1) {
    //
    // Wait for something to do
    //
    reply = redisCommand(rc, "BRPOP %s 0", p->key);
    if (reply == NULL) {
      fprintf(stderr, "Redis error: %s\n", rc->errstr);
      exit (-1);
    }

    if (reply->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "Redis brpop command produced an error: %s\n", reply->str);
      exit (-1);
    }
  
    //
    // The reply for brpop is an array with the name of the key as the
    // first element and the result in the second.
    //
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

    job = json_loads(subreply->str, 0, &jerr);
    if (job == NULL) {
      fprintf(stderr, "Failed to parse '%s': %s\n", subreply->str, jerr.text);
      continue;
    }
    freeReplyObject(reply);

    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);
    fprintf(stdout, "In Worker with Job '%s'\n", jobstr);
    free(jobstr);

    json_decref(job);
  }

  return NULL;
}

void isSupervisor(isProcessListType *p) {
  //
  // In child process running as user in home directory
  //
  static char buf[4096];  // buffer set to a fixed size that should be plenty big.  TODO: dynamically increase size if need be.
  struct pollfd pfds;
  int nread;
  char *in;
  char *out;
  json_t *command_obj;
  json_error_t jerr;
  int bytes_to_move;
  int fd;
  int i;
  int err;

  fprintf(stderr, "isSupervisor for process %s\n", p->key);

  fd =  p->req_pipe[0];
  in  = buf;
  out = buf;

  // Start up some workers
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_create(&(p->threads[i]), NULL, isWorker, p);
    if (err != 0) {
      fprintf(stderr, "Could not start worker for %s because %s\n", p->key, err==EAGAIN ? "Insufficient resources" : (err==EINVAL ? "Bad attributes" : (err==EPERM ? "No permission" : "Unknown Reasons")));
      return;
    }
    fprintf(stderr, "isSupervisor: Started worker %d\n", i+1);
  }

  // Wait for the workers to stop
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_join(&(p->threads[i]));
    switch(err) {
    case EDEADLK:
      fprintf(stderr, "isSupervisor: deadlock detected on join\n");
      break;
    case EINVAL:
      fprintf(stderr, "isSupervisor: threadi is unjoinable\n");
      break;
    case ESRCH:
      fprintf(stderr, "isSupervisor: thread could not be found\n");
      break;
    default:
    }
  }
  return;
}
