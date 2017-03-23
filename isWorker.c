#include "is.h"




void *isWorker(void *voidp) {
  static const char *id = FILEID "isWorker";
  isImageBufContext_t *ibctx;
  redisContext *rc;
  redisReply *reply;
  redisReply *subreply;
  json_t *job;
  json_error_t jerr;
  char *jobstr;
  const char *job_type;

  // make gcc happy
  ibctx = voidp;

  //
  // setup redis
  //
  rc = redisConnect("127.0.0.1", 6379);
  fflush(stdout);
  if (rc == NULL || rc->err) {
    fflush(stderr);

    if (rc) {
      fprintf(stderr, "%s: Failed to connect to redis: %s\n", id, rc->errstr);
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
    reply = redisCommand(rc, "BRPOP %s 0", ibctx->key);
    if (reply == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }

    if (reply->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis brpop command produced an error: %s\n", id, reply->str);
      exit (-1);
    }
  
    //
    // The reply for brpop is an array with the name of the key as the
    // first element and the result in the second.
    //
    if (reply->type != REDIS_REPLY_ARRAY) {
      fprintf(stderr, "%s: Redis brpop did not return an array, got type %d\n", id, reply->type);
      exit(-1);
    }
    
    if (reply->elements != 2) {
      fprintf(stderr, "%s: Redis bulk reply length should have been 2 but instead was %d\n", id, (int)reply->elements);
      exit(-1);
    }
    subreply = reply->element[1];
    if (subreply->type != REDIS_REPLY_STRING) {
      fprintf(stderr, "%s: Redis brpop did not return a string, got type %d\n", id, subreply->type);
      exit (-1);
    }

    if (strcmp(subreply->str, "end") == 0) {
      // "end" is special.  It means we must exit right away
      //
      freeReplyObject(reply);
      break;
    }


    job = json_loads(subreply->str, 0, &jerr);
    if (job == NULL) {
      fprintf(stderr, "%s: Failed to parse '%s': %s\n", id, subreply->str, jerr.text);
      freeReplyObject(reply);
      continue;
    }
    freeReplyObject(reply);

    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);

    if (json_integer_value(json_object_get(job, "esaf")) > 0) {
      fprintf(stderr, "%s: Got job %s\n", id, subreply->str);
    }

    job_type = json_string_value(json_object_get(job, "type"));
    if (job_type == NULL) {
      fprintf(stderr, "%s: No type parameter in job %s\n", id, jobstr);
    } else {
      // Cheapo command parser.  Probably the best way to go considering
      // the small number of commands we'll likely have to service.
      if (strcasecmp("jpeg", job_type) == 0) {
        isJpeg(ibctx, rc, job);
      } else {
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
  //
  // In child process running as user in home directory
  //
  isImageBufContext_t *ibctx;
  int i;
  int err;
  redisContext *rc;
  redisReply *reply;
  pthread_t threads[N_WORKER_THREADS];

  fprintf(stderr, "%s: start process %s\n", id, key);

  ibctx = isDataInit(key);

  // Start up some workers
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_create(&(threads[i]), NULL, isWorker, ibctx);

    if (err != 0) {
      fprintf(stderr, "%s: Could not start worker for %s because %s\n",
              id, key, err==EAGAIN ? "Insufficient resources" : (err==EINVAL ? "Bad attributes" : (err==EPERM ? "No permission" : "Unknown Reasons")));
      return;
    }
    fprintf(stderr, "%s: Started worker %d\n", id, i);
  }

  // Wait for the workers to stop
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_join(threads[i], NULL);
    switch(err) {
    case EDEADLK:
      fprintf(stderr, "%s: deadlock detected on join\n", id);
      break;
    case EINVAL:
      fprintf(stderr, "%s: threadi is unjoinable\n", id);
      break;
    case ESRCH:
      fprintf(stderr, "%s: thread could not be found\n", id);
      break;
    }
  }

  // free up the image buffers
  isDataDestroy(ibctx);

  // delete all the pending jobs
  rc = redisConnect("127.0.0.1", 6379);
  reply = redisCommand(rc, "DEL %s", key);
  if (reply == NULL) {
    fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
    exit (-1);
  }

  if (reply->type == REDIS_REPLY_ERROR) {
    fprintf(stderr, "%s: Redis brpop command produced an error: %s\n", id, reply->str);
    exit (-1);
  }
  
  freeReplyObject(reply);
  redisFree(rc);

  return;
}
