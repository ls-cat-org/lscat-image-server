/*! @file catchData.c
 *  @brief Worker to receive data from Eiger detector 
 *  @date 2017
 *  @author Keith Brister
 *  @copyright Northwestern University All Rights Reserved
 */
#include "is.h"

isImageBufContext_t  *isDataInit(const char *key) {
  static const char *id = FILEID "isDataInit";
  pthread_mutexattr_t matt;
  isImageBufContext_t *rtn;
  int err;

  fprintf(stderr, "%s: initializing imageBufferTable with %d entries\n", id, N_IMAGE_BUFFERS);


  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    fflush(stderr);
    exit (-1);
  }

  rtn->key = strdup(key);
  if (rtn->key == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    fflush(stderr);
    exit (-1);
  }

  rtn->n_buffers = 0;
  rtn->first     = NULL;

  pthread_mutexattr_init(&matt);
  pthread_mutexattr_settype(&matt, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutexattr_setpshared(&matt, PTHREAD_PROCESS_SHARED);
  pthread_mutex_init(&rtn->ctxMutex, &matt);
  pthread_mutexattr_destroy(&matt);

  errno = 0;
  err = hcreate_r( 2*N_IMAGE_BUFFERS, &rtn->bufTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to initialize hash table\n", id);
    fflush(stderr);
    exit(-1);
  }

  return rtn;
}

void isDataDestroy(isImageBufContext_t *c) {
  isImageBufType *p, *next;
  //
  // We are called from isSupervisor after all the threads have been
  // joined: there is no danger of collision and, hence, no need to
  // lock anything.
  //

  hdestroy_r(&c->bufTable);

  next = NULL;
  for (p=c->first; p!=NULL; p=next) {
    next = p->next;     // need to save next since we are going to free p
    if (p->rr) {
      freeReplyObject(p->rr);
      p->rr = NULL;
      p->buf = NULL;
    } else {
      if (p->buf) {
        free(p->buf);
        p->buf = NULL;
      }
    }
    free((char *)p->key);
    pthread_rwlock_destroy(&p->buflock);
    if (p->meta_str) {
      free(p->meta_str);
      p->meta_str = NULL;
    }
    free(p);
  }
  c->n_buffers = 0;
  c->first = NULL;
  pthread_mutex_destroy(&c->ctxMutex);
  free((char *)c->key);
  free(c);
}

image_access_type isFindFile(const char *fn) {
  static const char *id = FILEID "isFindFile";
  struct stat buf;
  image_file_type rtn;
  int fd;
  int err;
  int stat_errno;

  //
  // Oddly, we can open a file even if stat itself fails.  Stat needs
  // to have all the directories above the file to be accessable while
  // open does not.  Hence, we first open the file and then use fstat
  // instead of stat.
  //

  errno = 0;
  fd = open(fn, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "%s: Could not open file %s: %s\n", id, fn, strerror(errno));
    return NOACCESS;
  }

  errno = 0;
  err = fstat(fd, &buf);
  stat_errno = errno;
  close(fd);

  if (err != 0) {
    fprintf(stderr, "%s: Could not find file '%s': %s\n", id, fn, strerror(stat_errno));
    return NOACCESS;
  }
  
  if (!S_ISREG(buf.st_mode)) {
    fprintf(stderr, "%s: %s is not a regular file\n", id, fn);
    return NOACCESS;
  }

  //
  // Walk through the readable possibilities.  Consider that the
  // ownership and group privileges may be more restrictive than the
  // world privileges.
  //
  rtn = NOACCESS;
  if ((getuid() == buf.st_uid || geteuid() == buf.st_uid) && (buf.st_mode & S_IRUSR)) {
    rtn = READABLE;
  } else {
    if ((getgid() == buf.st_gid || getegid() == buf.st_gid) && (buf.st_mode & S_IRGRP)) {
      rtn = READABLE;
    } else {
      if (buf.st_mode & S_IROTH) {
        rtn = READABLE;
      }
    }
  }

  //
  // Test for writable priveleges: we consider the result as NOACCESS
  // if the file is writable without being readable.
  //
  if (rtn == (image_file_type)READABLE) {
    if ((getuid() == buf.st_uid || geteuid() == buf.st_uid) && (buf.st_mode & S_IWUSR)) {
      rtn = WRITABLE;
    } else {
      if ((getgid() == buf.st_gid || getegid() == buf.st_gid) && (buf.st_mode & S_IWGRP)) {
        rtn = WRITABLE;
      } else {
        if (buf.st_mode & S_IWOTH) {
          rtn = WRITABLE;
        }
      }
    }
  }
  return rtn;
}

image_file_type isFileType(const char *fn) {
  const char *id = FILEID "isFileType";
  htri_t ish5;
  herr_t herr;
  int fd;
  int nbytes;
  unsigned int buf4;

  fd = open(fn, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "%s: Could not open file '%s'\n", id, fn);
    return UNKNOWN;
  }

  nbytes = read(fd, (char *)&buf4, 4);
  close(fd);
  if (nbytes != 4) {
    fprintf(stderr, "%s: Could not read 4 bytes from file '%s'\n", id, fn);
    return UNKNOWN;
  }

  if (buf4 == 0x002a4949) {
    return RAYONIX;
  }

  if (buf4 == 0x49492a00) {
    return RAYONIX_BS;
  }

  //
  // H5 is easy
  //
  //
  // Turn off HDF5 error reporting.  The routines already return error codes 
  herr = H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not turn off HDF5 error reporting\n", id);
  }

  ish5 = H5Fis_hdf5(fn);
  herr = H5Eset_auto2(H5E_DEFAULT, (H5E_auto2_t) H5Eprint2, stderr);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not turn back on HDF5 error reporting\n", id);
  }

  if (ish5 > 0) {
    return HDF5;
  }

  fprintf(stderr, "%s: Unknown file type '%s'\n", id, fn);
  return UNKNOWN;
}

/** Create new buffer
 */
isImageBufType *createNewImageBuf(isImageBufContext_t *ibctx, const char *key) {
  static const char *id = FILEID "createNewImageBuf";
  ENTRY item;
  ENTRY *return_item;
  isImageBufType *rtn;
  int i;
  int err;
  isImageBufType *p;
  isImageBufType *last;
  isImageBufType *next;
  pthread_rwlockattr_t rwatt;

  // call with ctxMutex locked

  fprintf(stderr, "%s: 10\n", id);

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    fflush(stderr);
    exit (-1);
  }

  rtn->key = strdup(key);
  pthread_rwlockattr_init(&rwatt);
  pthread_rwlockattr_setpshared(&rwatt, PTHREAD_PROCESS_SHARED);
  pthread_rwlock_init(&rtn->buflock, &rwatt);
  pthread_rwlockattr_destroy(&rwatt);

  fprintf(stderr, "%s: Waiting for write lock for key %s\n", id, key);
  pthread_rwlock_wrlock(&rtn->buflock);
  fprintf(stderr, "%s: Got write lock for key %s\n", id, key);

  rtn->in_use = 1;              // we're going to be writing to this buffern then switching to reading it.  Will need to set in_use.

  rtn->next = ibctx->first;
  ibctx->first = rtn;

  item.key  = (char *)rtn->key;
  item.data = rtn;
  errno = 0;
  err = hsearch_r(item, ENTER, &return_item, &ibctx->bufTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to enter item %s: %s\n", id, rtn->key, strerror(errno));
    fflush(stderr);
    exit (-1);
  }
  
  fprintf(stderr, "%s: nbuffers=%d  N_IMAGE_BUFFERS=%d\n", id, ibctx->n_buffers, N_IMAGE_BUFFERS);

  if (++ibctx->n_buffers < N_IMAGE_BUFFERS) {
    fprintf(stderr, "%s: returning without rebuilding table.  key: %s\n", id, rtn->key);
    return rtn;
  }

  // Time to rebuild the hash table
  hdestroy_r(&ibctx->bufTable);
  errno = 0;
  err = hcreate_r( 2*N_IMAGE_BUFFERS, &ibctx->bufTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to initialize hash table\n", id);
    fflush(stderr);
    exit(-1);
  }
  //
  // Reenter the hash table values.  Keep some (say half) of the
  // most recent enties and all of the ones that are currently being
  // used
  //

  last = NULL;
  next = NULL;
  ibctx->n_buffers = 0;
  for(i=0, p=ibctx->first; p != NULL; p=next) {
    next = p->next;
    //
    // Don't destroy a buffer that is locked
    //
    if (i < N_IMAGE_BUFFERS/2 || p->in_use > 0) {
      ibctx->n_buffers++;
      p->next = last;
      item.key = (char *)p->key;
      item.data = p;
      last = p;
      err = hsearch_r(item, ENTER, &return_item, &ibctx->bufTable);
      if (err == 0) {
        fprintf(stderr, "%s: Failed to enter item %s\n", id, p->key);
        fflush(stderr);
        exit (-1);
      }
    } else {
      // we'll be removing this entry now
      //
      // TODO: sooner rather than later.  Correctly dispose of
      // "extra"
      if (p->rr) {
        //
        // Here p->rr->str is the buffer, free the redis object, not
        // the buffer
        //
        freeReplyObject(p->rr);
        p->rr = NULL;
        p->buf = NULL;
      } else {
        //
        // OK, buffer was malloced not redised (redis is a verb?)
        //
        if (p->buf) {
          free(p->buf);
          p->buf = NULL;
        }
      }

      free((char *)p->key);
      pthread_rwlock_destroy(&p->buflock);
      if (p->meta_str) {
        free(p->meta_str);
      }
      free(p);
    }
  }    


  // remember that return value we calculated so long ago?
  fprintf(stderr, "%s: returning after rebuilding table.  key: %s\n", id, rtn->key);
  return rtn;
}

/** Look to see if the data are already available to us from either
 * the image buffer hash table or from redis.  We will wait for the
 * data to appear if another process already is processing this.  In
 * this case we'll grab the data from redis when it's ready and
 * rerturn it.
 *
 *  When the data are availabe we'll return a read locked buffer.
 *  Otherwise, we'll returned with no data and a write locked buffer.
 *
 *  It is expected that if the caller has to go get the data
 *  themselves that they'll do the kindness of writing the data out to
 *  redis and add something to the list key-READY so that the
 *  processes can get back to work.
 *
 */
isImageBufType *isGetImageBufFromKey(isImageBufContext_t *ibctx, redisContext *rc, char *key) {
  static const char *id = FILEID "isGetImageBufFromKey";
  isImageBufType *rtn;          // This is our return value
  redisReply *rr;               // our redis reply object pointer
  redisReply *meta_rr;          // redis reply object perhaps with our metadata
  redisReply *width_rr;
  redisReply *height_rr;
  redisReply *depth_rr;
  redisReply *image_rr;         // redis reply object perhaps with our image data
  int need_to_read_data;        // non-zero when we are the first one asking for this data
  ENTRY item;
  ENTRY *return_item;
  int err;
  json_error_t jerr;

  rtn = NULL;

  fprintf(stderr, "%s: Waiting for ctxMutex\n", id);
  pthread_mutex_lock(&ibctx->ctxMutex);
  fprintf(stderr, "%s: Got ctxMutex\n", id);

  item.key  = key;
  item.data = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &return_item, &ibctx->bufTable);
  if (err != 0) {
    rtn = return_item->data;
  }
  
  if (rtn != NULL) {
    fprintf(stderr, "%s: Found buffer in bufTable for key %s with key %s\n", id, key, rtn->key);
    rtn->in_use++;                              // flag to keep buffer from being deleted before we get the read lock
    pthread_mutex_unlock(&ibctx->ctxMutex);
    fprintf(stderr, "%s: Unlocked ctxMutex\n", id);

    fprintf(stderr, "%s: Waiting for read lock for key (1) %s\n", id, key);
    pthread_rwlock_rdlock(&rtn->buflock);
    fprintf(stderr, "%s: Got read lock for key (1) %s\n", id, key);

    return rtn;
  }
  
  //
  // Create a new entry then read some data into it.  We still have
  // bufMutex locked: This is used to avoid contention with other
  // writers as well as potential readers.  We'll grab the read lock
  // on the buffer before releasing the context mutex.
  //
  rtn = createNewImageBuf(ibctx, key);
  // buffer is write locked and in_use = 1

  pthread_mutex_unlock(&ibctx->ctxMutex);       // But now we can allow access to the buffers
  fprintf(stderr, "%s: Unlocked ctxMutex after createnewImageBuf\n", id);

  //
  // Three redis queries follow:
  //  1) increment properties 'USERS' in for our key
  //  2) Set an expiration time for the key
  //  3) read data and meta properties
  //
  //  When the first query returns '1' then we know that the key did
  //  not exist before and that we'll need to find (or calculate) the
  //  data and then return it to the hash.
  //
  //  When the first query returns something greater than 1 then we'll
  //  run a blocking query to wait for the data to be written.
  //
  //  NB: any of our co-threads are already waiting for us to drop the
  //  write lock on the buffer before they go ahead.  All this magic
  //  here is to block other processes from redundantly processing the
  //  same data in the same way.
  //

  fprintf(stderr, "%s: attemping to get meta data for %s\n", id, key);
  redisAppendCommand(rc, "HINCRBY %s USERS 1", key);
  redisAppendCommand(rc, "EXPIRES %s %d", key, IS_REDIS_TTL);
  redisAppendCommand(rc, "HMGET %s META WIDTH HEIGHT DEPTH DATA", key);

  //
  // hincby reply: 1 means we are the first and should get the data, >1 means we should block until the data are there.
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hincby): %s\n", id, rc->errstr);
    exit (-1);
  }

  need_to_read_data = rr->integer == 1;
  fprintf(stderr, "%s: users = %d  reply type: %d\n", id, (int)rr->integer, rr->type);

  freeReplyObject(rr);

  fprintf(stderr, "%s: 10\n", id);

  //
  // expires reply: we don't need to do anything with this  err = redisGetReply(rc, &rr);
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hincby): %s\n", id, rc->errstr);
    exit (-1);
  }
  freeReplyObject(rr);

  fprintf(stderr, "%s: 20\n", id);

  //
  // Try to get the data.  If there is nothing to get then either get
  // it block until someone else gets it.
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hincby): %s\n", id, rc->errstr);
    exit (-1);
  }
  
  //
  // Here the reply is an array.  element[0] is the meta data (if any)
  // and element[1] is the image data (if any)
  //
  meta_rr   = rr->element[0];
  width_rr  = rr->element[1];
  height_rr = rr->element[2];
  depth_rr  = rr->element[3];
  image_rr  = rr->element[4];

  rtn->meta_str = meta_rr->type == REDIS_REPLY_STRING ? strdup(meta_rr->str) : NULL;
  rtn->meta = rtn->meta_str == NULL ? NULL : json_loads(rtn->meta_str, 0, &jerr);

  //  fprintf(stderr, "%s: key: %s  width: %d  width_type: %d height: %d  height_type: %d  depth: %d  depth_type: %d\n", id, rtn->key, (int)width_rr->integer, width_rr->type, (int)height_rr->integer, height_rr->type, (int)depth_rr->integer, depth_rr->type);

  rtn->buf_width  = atoi(width_rr->str);
  rtn->buf_height = atoi(height_rr->str);
  rtn->buf_depth  = atoi(depth_rr->str);

  rtn->extra = NULL;

  if (image_rr->type == REDIS_REPLY_STRING) {
    //
    // We're all set. We'll free the redis object after we are done
    // with the image.  We'll also exchange our write lock for a read
    // lock.
    rtn->rr  = rr;
    rtn->buf = image_rr->str;
    rtn->buf_size = image_rr->len;

    if (rtn->buf_size != rtn->buf_width * rtn->buf_height * rtn->buf_depth) {
      fprintf(stderr, "%s: Bad buffer size.  Width: %d  Height: %d  Depth: %d  Size: %d\n", id, rtn->buf_width, rtn->buf_height, rtn->buf_depth, rtn->buf_size);
      exit (-1);
    }

    pthread_rwlock_unlock(&rtn->buflock);
    fprintf(stderr, "%s: Gave up lock for key %s\n", id, key);

    fprintf(stderr, "%s: Waiting for read lock for key (2) %s\n", id, key);
    pthread_rwlock_rdlock(&rtn->buflock);
    fprintf(stderr, "%s: Got read lock for key (2) %s\n", id, key);

    freeReplyObject(rr);

    fprintf(stderr, "%s: 25\n", id);

    return rtn;

  }
  // OK, no data yet.
  freeReplyObject(rr);

  fprintf(stderr, "%s: 30\n", id);

  if (need_to_read_data) {
    // Who ever called us will go ahead and fill the image data if we
    // return a null buffer.
    //
    // In this case we hold on to the write lock.
    //

    fprintf(stderr, "%s: 32\n", id);
    return rtn;
  }

  // Now we just wait for the data to magically appear
  //
  // TODO: evaluate and perhaps implement a timeout and error recovery
  //
  fprintf(stderr, "%s: About to wait for someone else to write the data\n", id);
  rr = redisCommand(rc, "BRPOP %s-READY 0", key);
  if (rr == NULL) {
    fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
    exit (-1);
  }
  if (rr->type == REDIS_REPLY_STRING && strcmp(rr->str, "error")==0) {
    //
    // Something bad happened to whoever tried to read the data file.
    //
    // TODO: Pass the error condition on to whoever called us cause as
    // it is they'll just try reading the file too.
    //
    freeReplyObject(rr);
    fprintf(stderr, "%s: 35\n", id);
    return rtn;
  }

  freeReplyObject(rr);
  fprintf(stderr, "%s: 40\n", id);

  rr = redisCommand(rc, "HMGET %s META WIDTH HEIGHT DEPTH DATA", key);
  if (rr == NULL) {
    fprintf(stderr, "%s: Redis failure (hincby): %s\n", id, rc->errstr);
    exit (-1);
  }

  meta_rr   = rr->element[0];
  width_rr  = rr->element[1];
  height_rr = rr->element[2];
  depth_rr  = rr->element[3];
  image_rr  = rr->element[4];

  rtn->meta_str = meta_rr->type == REDIS_REPLY_STRING ? strdup(meta_rr->str) : NULL;
  rtn->meta = rtn->meta_str == NULL ? NULL : json_loads(rtn->meta_str, 0, &jerr);

  if (image_rr->type == REDIS_REPLY_STRING) {
    //
    // We're all set. We'll free the redis object after we are done
    // with the image.  We'll also exchange our write lock for a read
    // lock.
    rtn->rr  = rr;
    rtn->buf = image_rr->str;
    rtn->buf_size = image_rr->len;

    rtn->buf_width  = atoi(width_rr->str);
    rtn->buf_height = atoi(height_rr->str);
    rtn->buf_depth  = atoi(depth_rr->str);

    if (rtn->buf_size != rtn->buf_width * rtn->buf_height * rtn->buf_depth) {
      fprintf(stderr, "%s: Bad buffer size.  Width: %d  Height: %d  Depth: %d  Size: %d\n", id, rtn->buf_width, rtn->buf_height, rtn->buf_depth, rtn->buf_size);
      exit (-1);
    }

    fprintf(stderr, "%s: Gave up lock for key %s\n", id, key);
    pthread_rwlock_unlock(&rtn->buflock);

    fprintf(stderr, "%s: Waiting for read lock for key (3) %s\n", id, key);
    pthread_rwlock_rdlock(&rtn->buflock);
    fprintf(stderr, "%s: Got read lock for key (3) %s\n", id, key);

    fprintf(stderr, "%s: 45\n", id);
    return rtn;

  }

  // OK, no data yet.  Don't cry.
  freeReplyObject(rr);

  fprintf(stderr, "%s: 50\n", id);
  return rtn;
}

void isWriteImageBufToRedis(isImageBufType *imb, redisContext *rc) {
  static const char *id = FILEID "isWriteImageBufToRedis";
  redisReply *rr;
  int err;
  int i;
  int n;

  fprintf(stderr, "%s: Here I am with Key %s and buf_size %d\n", id, imb->key, imb->buf_size);

  redisAppendCommand(rc, "HMSET %s META %s WIDTH %d HEIGHT %d DEPTH %d", imb->key, imb->meta_str, imb->buf_width, imb->buf_height, imb->buf_depth);
  redisAppendCommand(rc, "HSET %s DATA %b", imb->key, imb->buf, imb->buf_size);
  redisAppendCommand(rc, "HGET %s USERS", imb->key);

  // meta reply
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hset meta): %s\n", id, rc->errstr);
    exit (-1);
  }
  freeReplyObject(rr);

  // data reply
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hset data): %s\n", id, rc->errstr);
    exit (-1);
  }
  freeReplyObject(rr);

  // users reply
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hget users): %s\n", id, rc->errstr);
    exit (-1);
  }
  n = rr->integer;
  freeReplyObject(rr);
  fprintf(stderr, "%s: There are %d users,  %d waiting\n", id, n, n-1);
  
  //
  // tell the processes awaiting notification it's time to get back to work
  //
  for (i=1; i<n; i++) {
    rr = redisCommand(rc, "LPUSH %s-READY ok", imb->key);
    freeReplyObject(rr);
  }

  fprintf(stderr, "%s: There I go with Key %s\n", id, imb->key);

}

/**
 */
isImageBufType *isGetRawImageBuf(isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isGetRawImageBuf";
  const char *fn;
  int frame;
  int frame_strlen;
  isImageBufType *rtn;
  int gid;
  int gid_strlen;
  char *key;
  int key_strlen;
  image_file_type ft;

  fn = json_string_value(json_object_get(job, "fn"));
  if (fn == NULL || strlen(fn) == 0) {
    return NULL;
  }

  gid = getegid();
  if (gid <= 0) {
    fprintf(stderr, "%s: Bad gid %d\n", id, gid);
    return NULL;
  }
  gid_strlen = ((int)log10(gid)) + 1;

  frame = json_integer_value(json_object_get(job,"frame"));
  if (frame <= 0) {
    frame = 1;
  }

  frame_strlen = ((int)log10(frame)) + 1;

  //           length of strings plus 2 slashes and one dash  
  key_strlen = strlen(fn) + gid_strlen + frame_strlen +  3;
  key = calloc(1,  key_strlen + 1);
  if (key == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    fflush(stderr);
    exit (-1);
  }
  snprintf(key, key_strlen, "%d:%s-%d", gid, fn, frame);
  key[key_strlen] = 0;

  // Get the buffer and fill it if it's in redis already
  //
  // Buffer is read locked if it exists, write locked if it does not
  //
  fprintf(stderr, "%s: about to get image buffer from key %s\n", id, key);

  rtn = isGetImageBufFromKey(ibctx, rc, key);
  if (rtn->buf != NULL) {
    fprintf(stderr, "%s: Found buffer for key %s\n", id, key);
    free(key);
    return rtn;
  }
  
  fprintf(stderr, "%s: About to try and read file %s\n", id, fn);
  // I guess we didn't find our buffer in redis
  //
  // META does not exist or was never entered.  Re-read both meta and data.
  ft = isFileType(fn);
  switch (ft) {
  case HDF5:
    rtn->meta = isH5GetMeta(fn);
    isH5GetData(fn, frame, rtn);
    break;
      
  case RAYONIX:
  case RAYONIX_BS:
    rtn->meta = isRayonixGetMeta(fn);
    isRayonixGetData(fn, frame, rtn);
    break;
      
  case UNKNOWN:
  default:
    fprintf(stderr, "%s: unknown file type for file %s\n", id, fn);
    pthread_rwlock_unlock(&rtn->buflock);
    fprintf(stderr, "%s: Gave up lock for key %s\n", id, key);
    free(key);
    return NULL;
  }

  fprintf(stderr, "%s: read file %s\n", id, fn);

  rtn->meta_str = json_dumps(rtn->meta, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  if (rtn->meta_str == NULL) {
    fprintf(stderr, "%s: Could not stringify meta object.  This is not supposed to be possible\n", id);
    exit (-1);
  }

  isWriteImageBufToRedis(rtn, rc);

  pthread_rwlock_unlock(&rtn->buflock);
  fprintf(stderr, "%s: Gave up lock for key %s\n", id, key);
  fprintf(stderr, "%s: Waiting for read lock for key (4) %s\n", id, key);
  pthread_rwlock_rdlock(&rtn->buflock);
  fprintf(stderr, "%s: Got read lock for key (4) %s\n", id, key);

  return rtn;
}
