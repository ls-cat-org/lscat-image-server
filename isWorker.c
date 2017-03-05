#include "is.h"

void isWorkerJpeg(json_t *job) {
  const char *fn;       // file name from job.
  image_access_type file_access;       // file descriptor for this file
  image_file_type   file_type;

  fn = json_string_value(json_object_get(job, "fn"));
  if (fn == NULL) {
    fprintf(stderr, "isWorkerJpeg: Could not find file name parameter (fn) in job.\n");
    return;
  }

  file_access = NOACCESS;
  file_type   = BLANK;
  if (strlen(fn) > 0) {
    file_access = isFindFile(fn);
    if (file_access == NOACCESS) {
      fprintf(stderr, "isWorkerJpeg: Can not access file %s\n", fn);
      return;
    }
    file_type = isFileType(fn);
  }
  
  switch (file_type) {
  case HDF5:
    isH5Jpeg(job);
    break;
    
  case RAYONIX:
  case RAYONIX_BS:
    isRayonixJpeg(job);
    break;

  case BLANK:
    isBlankJpeg(job);
    break;

  case UNKNOWN:
  default:
    fprintf(stderr, "isWorkerJpeg: Ignoring unknown file type '%s'\n", fn);
  }
  return;
}

void *isWorker(void *voidp) {
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
      fprintf(stderr, "Failed to connect to redis: %s\n", rc->errstr);
    } else {
      fprintf(stderr, "Failed to get redis context\n");
    }
    exit (-1);
  }

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
      freeReplyObject(reply);
      continue;
    }
    freeReplyObject(reply);

    jobstr = json_dumps(job, JSON_INDENT(0) | JSON_COMPACT | JSON_SORT_KEYS);
    //    fprintf(stdout, "In Worker with Job '%s'\n", jobstr);
    
    job_type = json_string_value(json_object_get(job, "type"));
    if (job_type == NULL) {
      fprintf(stderr, "No type parameter in job %s\n", jobstr);
    } else {
      // Cheapo command parser.  Probably the best way to go considering
      // the small number of commands we'll likely have to service.
      if (strcasecmp("jpeg", job_type) == 0) {
        isWorkerJpeg(job);
      } else {
        fprintf(stderr, "Unknown job type '%s' in job '%s'\n", job_type, jobstr);
      }
    }
    free(jobstr);
    json_decref(job);
  }
  return NULL;
}

void isSupervisor(isProcessListType *p) {
  //
  // In child process running as user in home directory
  //
  int i;
  int err;

  fprintf(stderr, "isSupervisor for process %s\n", p->key);

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
    err = pthread_join(p->threads[i], NULL);
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
    }
  }
  return;
}
