#include "is.h"

void isWorkerJpeg(redisContext *rc, json_t *job) {
  static const char *id = FILEID "isWorkerJpeg";
  const char *fn;                       // file name from job.
  isImageBufType *imb;
  int frame;

  fn = json_string_value(json_object_get(job, "fn"));
  if (fn == NULL) {
    fprintf(stderr, "isWorkerJpeg: Could not find file name parameter (fn) in job.\n");
    return;
  }

  frame = json_integer_value(json_object_get(job, "frame"));
  if (frame == 0) {
    //
    // We'll see 0 if frame is not  defined.  Just set it to 1 as that
    // frame should always exist.
    //
    frame = 1;
  }

  if (strlen(fn) == 0) {
    //
    // Don't bother with the rest of this if all we want is a blank jpeg
    //
    isBlankJpeg(job);
    return;
  }


  imb = isGetImageBuf(rc, job);
  if (imb == NULL) {
    fprintf(stderr, "%s: Could not find data for frame %d in file %s\n", id, frame, fn);
    return;
  }


  // TODO: go process the jpeg already, what's keeping you?
  return;
}

void *isWorker(void *voidp) {
  static const char *id = FILEID "isWorker";
  isProcessListType *p;
  redisContext *rc;
  redisReply *reply;
  redisReply *subreply;
  json_t *job;
  json_error_t jerr;
  char *jobstr;
  const char *job_type;

  // make gcc happy
  p = voidp;

  //
  // setup redis
  //
  rc = redisConnect("localhost", 6379);
  if (rc == NULL || rc->err) {
    if (rc) {
      fprintf(stderr, "%s: Failed to connect to redis: %s\n", id, rc->errstr);
    } else {
      fprintf(stderr, "%s: Failed to get redis context\n", id);
    }
    exit (-1);
  }

  while (1) {
    //
    // Wait for something to do
    //
    reply = redisCommand(rc, "BRPOP %s 0", p->key);
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

    job = json_loads(subreply->str, 0, &jerr);
    if (job == NULL) {
      fprintf(stderr, "%s: Failed to parse '%s': %s\n", id, subreply->str, jerr.text);
      freeReplyObject(reply);
      continue;
    }
    freeReplyObject(reply);

    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);
    
    job_type = json_string_value(json_object_get(job, "type"));
    if (job_type == NULL) {
      fprintf(stderr, "%s: No type parameter in job %s\n", id, jobstr);
    } else {
      // Cheapo command parser.  Probably the best way to go considering
      // the small number of commands we'll likely have to service.
      if (strcasecmp("jpeg", job_type) == 0) {
        isWorkerJpeg(rc, job);
      } else {
        fprintf(stderr, "%s: Unknown job type '%s' in job '%s'\n", id, job_type, jobstr);
      }
    }
    free(jobstr);
    json_decref(job);
  }
  return NULL;
}

void isSupervisor(isProcessListType *p) {
  static const char *id = FILEID "isSupervisor";
  //
  // In child process running as user in home directory
  //
  int i;
  int err;

  fprintf(stderr, "%s: start process %s\n", id, p->key);

  // Start up some workers
  for (i=1; i<N_WORKER_THREADS; i++) {
    err = pthread_create(&(p->threads[i]), NULL, isWorker, p);
    if (err != 0) {
      fprintf(stderr, "%s: Could not start worker for %s because %s\n", id, p->key, err==EAGAIN ? "Insufficient resources" : (err==EINVAL ? "Bad attributes" : (err==EPERM ? "No permission" : "Unknown Reasons")));
      return;
    }
    fprintf(stderr, "%s: Started worker %d\n", id, i);
  }

  // Wait for the workers to stop
  for (i=0; i<N_WORKER_THREADS; i++) {
    err = pthread_join(p->threads[i], NULL);
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
  return;
}
