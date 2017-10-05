/*! @file isData.c
 *  @brief Routines to deal with our image buffers
 *  @date 2017
 *  @author Keith Brister
 *  @copyright Northwestern University All Rights Reserved
 */
#include "is.h"

/** Release image buffer contents
 * 
 * Call with the context in which this buffer was defined locked.
 */
void destroyImageBuffer(isImageBufType *p) {
  static const char *id = FILEID "destroyImageBuffer";
  (void)id;

  fprintf(stdout, "%s: destroying image buffer %s\n", id, p->key);

  if (p->rr) {
    // The buffer was from redis: buf and bad pixel map belong to
    // p->rr
    //
    freeReplyObject(p->rr);
    p->rr  = NULL;
    p->buf = NULL;
    p->bad_pixel_map = NULL;
  } else {
    // The buffer was from a file: buf and bad pixel map (if it
    // exists) were malloc'ed
    //
    if (p->buf) {
      free(p->buf);
      p->buf = NULL;
    }
    if (p->bad_pixel_map) {
      free(p->bad_pixel_map);
      p->bad_pixel_map = NULL;
    }
  }
  free((char *)p->key);
  pthread_rwlock_destroy(&p->buflock);
  if (p->meta) {
    json_decref(p->meta);
    p->meta = NULL;
  }
  free(p);
  fprintf(stdout, "%s: done\n", id);
}

/** Initialize an process's image buffer context
 */
isWorkerContext_t  *isDataInit(const char *key) {
  static const char *id = FILEID "isDataInit";
  pthread_mutexattr_t matt;
  isWorkerContext_t *rtn;
  int err;
  char router_endpoint[128];
  char dealer_endpoint[128];
  int socket_option;

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->key = strdup(key);
  if (rtn->key == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->n_buffers   = 0;
  rtn->max_buffers = 2 * N_IMAGE_BUFFERS;
  rtn->first     = NULL;

  rtn->zctx = zmq_ctx_new();
  rtn->router = zmq_socket(rtn->zctx, ZMQ_ROUTER);
  if (rtn->router == NULL) {
    fprintf(stderr, "%s: Could not create router socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->router, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set RCVHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->router, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set SNDHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(router_endpoint, sizeof(router_endpoint)-1, "ipc://@%s", key);
  router_endpoint[sizeof(router_endpoint)-1] = 0;

  err = zmq_connect(rtn->router, router_endpoint);
  if (err == -1) {
    fprintf(stderr, "%s: failed to connect to endpoint %s: %s\n", id, router_endpoint, strerror(errno));
    exit (-1);
  }
                    
  rtn->dealer = zmq_socket(rtn->zctx, ZMQ_DEALER);
  if (rtn->dealer == NULL) {
    fprintf(stderr, "%s: Could not create dealer socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->dealer, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set RCVHWM for dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->dealer, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set SNDHWM for dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "inproc://#%s", key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;
  err = zmq_bind(rtn->dealer, dealer_endpoint);
  if (err == -1) {
    fprintf(stderr, "%s: Could not bind dealer endpoint %s: %s\n", id, dealer_endpoint, strerror(errno));
    exit (-1);
  }
  
  pthread_mutexattr_init(&matt);
  pthread_mutexattr_settype(&matt, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutexattr_setpshared(&matt, PTHREAD_PROCESS_SHARED);
  pthread_mutex_init(&rtn->ctxMutex, &matt);
  pthread_mutexattr_destroy(&matt);

  err = hcreate_r( 2*N_IMAGE_BUFFERS, &rtn->bufTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to initialize hash table: %s\n", id, strerror(errno));
    exit (-1);
  }

  return rtn;
}

/** Recycle resources garnered by isDataInit
 */
void isDataDestroy(isWorkerContext_t *c) {
  static const char *id = FILEID "isDataDestroy";
  isImageBufType *p, *next;
  (void)id;

  fprintf(stdout, "%s: start\n", id);
  //
  // We are called from isSupervisor after all the threads have been
  // joined: there is no danger of collision and, hence, no need to
  // lock anything.
  //

  hdestroy_r(&c->bufTable);

  next = NULL;
  for (p=c->first; p!=NULL; p=next) {
    next = p->next;     // need to save next since p is going away.
    destroyImageBuffer(p);
  }
  c->n_buffers = 0;
  c->first = NULL;
  pthread_mutex_destroy(&c->ctxMutex);
  free((char *)c->key);
  free(c);
  fprintf(stdout, "%s: Done\n", id);
}

/** Locate file and make sure we can read it.
 *
 *  @todo Look in other likely places incase the file isn't where we
 *  think is should be. That is, if it's on another file system
 *  (perhaps compressed) we should just deal with it instead of return
 *  an error.
 *
 */
image_access_type isFindFile(const char *fn) {
  static const char *id = FILEID "isFindFile";
  struct stat buf;
  image_file_type rtn;
  int fd;
  int err;
  int stat_errno;

  //
  // Oddly, we can open a file even when stat itself would fail.
  // Stat needs to have all the directories above the file to be
  // accessable while open does not.  Hence, we first open the file
  // and then use fstat instead of stat.
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
  close(fd);                    // let's not be leaking file descriptors

  if (err != 0) {
    fprintf(stderr, "%s: Could not find file '%s': %s\n", id, fn, strerror(stat_errno));
    return NOACCESS;
  }
  
  if (!S_ISREG(buf.st_mode)) {
    fprintf(stderr, "%s: %s is not a regular file\n", id, fn);
    return NOACCESS;
  }

  //
  // Walk through the readability possibilities.  Consider that the
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

/** Figure out what sort of image we have.  We do this by reading a
 * few bytes of the file and comparing them with a list of known file
 * types, or in the case of hdf5, we just use the provided API.  This
 * gets around oddities associated with file naming conventions.
 */
image_file_type isFileType(const char *fn) {
  const char *id = FILEID "isFileType";
  htri_t ish5;
  herr_t herr;
  int fd;
  int nbytes;
  unsigned int buf4;

  errno = 0;
  fd = open(fn, O_RDONLY);
  if (fd == -1) {
    fprintf(stderr, "%s: Could not open file '%s'  uid: %d  gid: %d  error: %s\n", id, fn, geteuid(), getegid(), strerror(errno));
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

  herr = H5Eset_auto2(H5E_DEFAULT, (H5E_auto2_t) H5Eprint2, stderr);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not turn back on HDF5 error reporting\n", id);
  }

  ish5 = H5Fis_hdf5(fn);
  if (ish5 > 0) {
    return HDF5;
  }

  fprintf(stderr, "%s: Unknown file type '%s' buf4 = %x08\n", id, fn, buf4);
  return UNKNOWN;
}

/** Create new buffer
 *
 * Call with wctx->ctxMutex locked
 *
 * Return with a brand new write locked image buffer and "in_use" set
 * to 1 to keep the buffer from being reclaimed when we give up our
 * write lock in favor of a read lock.
 */
isImageBufType *createNewImageBuf(isWorkerContext_t *wctx, const char *key) {
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
  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->key = strdup(key);
  pthread_rwlockattr_init(&rwatt);
  pthread_rwlockattr_setpshared(&rwatt, PTHREAD_PROCESS_SHARED);
  pthread_rwlock_init(&rtn->buflock, &rwatt);
  pthread_rwlockattr_destroy(&rwatt);

  pthread_rwlock_wrlock(&rtn->buflock);

  rtn->in_use = 1;      // in_use is protected by wctx->ctcMutex

  rtn->next = wctx->first;
  wctx->first = rtn;

  item.key  = (char *)rtn->key;
  item.data = rtn;
  errno = 0;
  err = hsearch_r(item, ENTER, &return_item, &wctx->bufTable);
  //
  // An error here means that we've somehow exceeded the maximum
  // number of entries in the table.  We should have at least a factor
  // of 2 head room so this is a serious programming error.
  //
  if (err == 0) {
    fprintf(stderr, "%s: Failed to enter item %s: %s\n", id, rtn->key, strerror(errno));
    exit (-1);
  }

  if (++wctx->n_buffers < wctx->max_buffers / 2) {
    //
    // We are done.
    //
    return rtn;
  }

  // Time to rebuild the hash table
  //
  // Count number of buffers still in use
  //
  fprintf(stdout, "%s: Rebuilding hash table\n", id);

  for(i=0, p=wctx->first; p != NULL; p=p->next) {
    assert(p->in_use >= 0);
    if (p->in_use > 0) {
      i++;
    }
  }

  //
  // Guess how many buffers we need and double that to make the hash table more efficient.
  // TODO: Make a better guess by trying to understand what hcreate really needs.
  //
  wctx->max_buffers += 2 * (N_IMAGE_BUFFERS + i);

  fprintf(stdout, "%s: ************ Rebuilding hash table. i=%d, max_buffers=%d\n", id, i, wctx->max_buffers);

  hdestroy_r(&wctx->bufTable);
  errno = 0;
  err = hcreate_r( wctx->max_buffers, &wctx->bufTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to initialize hash table\n", id);
    exit (-1);
  }
  //
  // Reenter the hash table values.  Keep some (say half) of the
  // most recent enties and all of the ones that are currently being
  // used
  //

  last = NULL;
  next = NULL;
  wctx->n_buffers = 0;
  for(i=0, p=wctx->first; p != NULL; p=next) {
    next = p->next;

    assert(p->in_use >= 0);

    if (i++ > wctx->max_buffers/4 && p->in_use <= 0) {
      destroyImageBuffer(p);
      continue;
    }

    wctx->n_buffers++;

    if (last != NULL) {
      last->next = p;
    }
    item.key = (char *)p->key;
    item.data = p;
    last = p;
    err = hsearch_r(item, ENTER, &return_item, &wctx->bufTable);
    if (err == 0) {
      fprintf(stderr, "%s: Failed to enter item %s\n", id, p->key);
      exit (-1);
    }
  }    

  // remember that return value we calculated so long ago?
  //fprintf(stdout, "%s: returning after rebuilding table.  key: %s\n", id, rtn->key);
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
isImageBufType *isGetImageBufFromKey(isWorkerContext_t *wctx, redisContext *rc, char *key) {
  static const char *id = FILEID "isGetImageBufFromKey";
  isImageBufType *rtn;          // This is our return value
  redisReply *rr;               // our redis reply object pointer
  redisReply *meta_rr;          // redis reply object perhaps with our metadata
  redisReply *width_rr;
  redisReply *height_rr;
  redisReply *depth_rr;
  redisReply *image_rr;         // redis reply object perhaps with our image data
  redisReply *badpixels_rr;     // redis reply object perhaps with our bad pixel map
  int need_to_read_data;        // non-zero when we are the first one asking for this data
  ENTRY item;
  ENTRY *return_item;
  int err;
  json_error_t jerr;

  rtn = NULL;

  pthread_mutex_lock(&wctx->ctxMutex);

  item.key  = key;
  item.data = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &return_item, &wctx->bufTable);
  if (err != 0) {
    rtn = return_item->data;
  }
  
  if (rtn != NULL) {

    assert(rtn->in_use >= 0);

    rtn->in_use++;                              // flag to keep our buffer in scope while we need it
    pthread_mutex_unlock(&wctx->ctxMutex);
    pthread_rwlock_rdlock(&rtn->buflock);

    return rtn;
  }
  
  //
  // Create a new entry then read some data into it.  We still have
  // bufMutex write locked: This is used to avoid contention with
  // other writers as well as potential readers.  We'll grab the read
  // lock on the buffer before releasing the context mutex.
  //
  rtn = createNewImageBuf(wctx, key);
  // buffer is write locked and in_use = 1

  pthread_mutex_unlock(&wctx->ctxMutex);       // We can now allow access to the other buffers

  #ifdef IS_IGNORE_REDIS_STORE
  //
  // Just return now if we have not found a buffer.  Our caller will
  // fill.  Leave with buffer write locked and in_use non-zero
  //
  return rtn;
  #endif

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
  //  NB: Possibly one or more of our co-threads are already waiting
  //  for us to drop the write lock on the buffer before they go
  //  ahead.  All this magic here is to block other processes from
  //  redundantly processing the same data in the same way.
  //

  redisAppendCommand(rc, "HINCRBY %s USERS 1", key);
  redisAppendCommand(rc, "EXPIRE %s %d", key, IS_REDIS_TTL);
  redisAppendCommand(rc, "HMGET %s META WIDTH HEIGHT DEPTH DATA BADPIXELS", key);

  //
  // hincby reply: 1 means we are the first and should get the data,
  // >1 means we should block until the data are first written by
  // another process.
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hincby): %s\n", id, rc->errstr);
    exit (-1);
  }

  need_to_read_data = rr->integer == 1;
  //fprintf(stdout, "%s: users = %d  reply type: %d\n", id, (int)rr->integer, rr->type);

  freeReplyObject(rr);

  //
  // expire reply: we don't need to do anything with this  err = redisGetReply(rc, &rr);
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (expire): %s\n", id, rc->errstr);
    exit (-1);
  }
  freeReplyObject(rr);

  //
  // Try to get the data.  If there is nothing to get then either get
  // it block until someone else gets it.
  //
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hmget): %s\n", id, rc->errstr);
    exit (-1);
  }
  
  //
  // Here the reply is an array.  element[0] is the meta data (if any)
  // and element[1] is the image data (if any)
  //
  meta_rr      = rr->element[0];
  width_rr     = rr->element[1];
  height_rr    = rr->element[2];
  depth_rr     = rr->element[3];
  image_rr     = rr->element[4];
  badpixels_rr = rr->element[5];

  rtn->meta = NULL;
  if (meta_rr->type == REDIS_REPLY_STRING) {
    rtn->meta = json_loads(meta_rr->str, 0, &jerr);
  }

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

    rtn->bad_pixel_map = NULL;
    if (badpixels_rr->type == REDIS_REPLY_STRING) {
      rtn->bad_pixel_map = badpixels_rr->str;
    }

    rtn->extra = NULL;

    if (rtn->buf_size != rtn->buf_width * rtn->buf_height * rtn->buf_depth) {
      fprintf(stderr, "%s: Bad buffer size.  Width: %d  Height: %d  Depth: %d  Size: %d\n", id, rtn->buf_width, rtn->buf_height, rtn->buf_depth, rtn->buf_size);
      exit (-1);
    }

    pthread_rwlock_unlock(&rtn->buflock);
    pthread_rwlock_rdlock(&rtn->buflock);

    freeReplyObject(rr);
    return rtn;
  }
  // OK, no data yet.
  freeReplyObject(rr);

  if (need_to_read_data) {
    // Who ever called us will go ahead and fill the image data if we
    // return a null buffer.
    //
    // In this case we hold on to the write lock (and a non-zero in_use)
    //
    return rtn;
  }

  // Now we just wait for the data to magically appear
  //
  // TODO: evaluate and perhaps implement a timeout with error recovery
  //

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
    return rtn;
  }

  freeReplyObject(rr);

  redisAppendCommand(rc, "EXPIRE %s %d", key, IS_REDIS_TTL);
  redisAppendCommand(rc, "HMGET %s META WIDTH HEIGHT DEPTH DATA BADPIXELS", key); 

  // Expire
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (expire): %s\n", id, rc->errstr);
    exit (-1);
  }
  freeReplyObject(rr);

  // Mget
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (mget): %s\n", id, rc->errstr);
    exit (-1);
  }

  meta_rr      = rr->element[0];
  width_rr     = rr->element[1];
  height_rr    = rr->element[2];
  depth_rr     = rr->element[3];
  image_rr     = rr->element[4];
  badpixels_rr = rr->element[5];

  rtn->meta = NULL;
  if (meta_rr->type == REDIS_REPLY_STRING) {
    rtn->meta = json_loads(meta_rr->str, 0, &jerr);
  }


  if (image_rr->type != REDIS_REPLY_STRING) {
    //
    // Perhaps the other process had a problem.  We'll just return as
    // we are and let our caller deal with it.
    //
    freeReplyObject(rr);
    return rtn;
  }

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
  
  rtn->bad_pixel_map = NULL;
  if (badpixels_rr->type == REDIS_REPLY_STRING) {
    rtn->bad_pixel_map = badpixels_rr->str;
  }
  
  if (rtn->buf_size != rtn->buf_width * rtn->buf_height * rtn->buf_depth) {
    fprintf(stderr, "%s: Bad buffer size.  Width: %d  Height: %d  Depth: %d  Size: %d\n", id, rtn->buf_width, rtn->buf_height, rtn->buf_depth, rtn->buf_size);
    exit (-1);
  }
  pthread_rwlock_unlock(&rtn->buflock);
  pthread_rwlock_rdlock(&rtn->buflock);
  
  return rtn;
}

/**  After we've read some data we'll place it in redis so that other
 *   processes can access it without having to read the original file
 *   and/or re-reduce it.
 */
void isWriteImageBufToRedis(isImageBufType *imb, redisContext *rc) {
  static const char *id = FILEID "isWriteImageBufToRedis";
  redisReply *rr;
  char *meta_str;
  int err;
  int i;
  int n;

  meta_str = json_dumps(imb->meta, JSON_COMPACT | JSON_INDENT(0) | JSON_SORT_KEYS);

  redisAppendCommand(rc, "HMSET %s META %s WIDTH %d HEIGHT %d DEPTH %d", imb->key, meta_str, imb->buf_width, imb->buf_height, imb->buf_depth);
  redisAppendCommand(rc, "HSET %s DATA %b", imb->key, imb->buf, imb->buf_size);
  if (imb->bad_pixel_map) {
    redisAppendCommand(rc, "HSET %s BADPIXELS %b", imb->key, imb->bad_pixel_map, sizeof(uint32_t) * imb->buf_width * imb->buf_height);
  }
  redisAppendCommand(rc, "EXPIRE %s %d", imb->key, IS_REDIS_TTL);
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

  // badpixels reply
  if (imb->bad_pixel_map != NULL) {
    err = redisGetReply(rc, (void **)&rr);
    if (err != REDIS_OK) {
      fprintf(stderr, "%s: Redis failure (hset badpixels): %s\n", id, rc->errstr);
      exit (-1);
    }
    freeReplyObject(rr);
  }    

  // expire reply
  err = redisGetReply(rc, (void **)&rr);
  if (err != REDIS_OK) {
    fprintf(stderr, "%s: Redis failure (hset expire): %s\n", id, rc->errstr);
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
  
  //
  // tell the processes awaiting notification it's time to get back to work
  //
  for (i=1; i<n; i++) {
    rr = redisCommand(rc, "LPUSH %s-READY ok", imb->key);
    freeReplyObject(rr);
  }
}

/** Get the unreduced image
 */
isImageBufType *isGetRawImageBuf(isWorkerContext_t *wctx, redisContext *rc, json_t *job) {
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
    exit (-1);
  }
  snprintf(key, key_strlen, "%d:%s-%d", gid, fn, frame);
  key[key_strlen] = 0;

  // Get the buffer and fill it if it's in redis already
  //
  // Buffer is read locked if it exists, write locked if it does not
  //
  // fprintf(stdout, "%s: about to get image buffer from key %s\n", id, key);

  rtn = isGetImageBufFromKey(wctx, rc, key);
  rtn->frame = frame;
  if (rtn->buf != NULL) {
    fprintf(stderr, "%s: Found buffer for key %s\n", id, key);
    free(key);
    return rtn;
  }
  
  // I guess we didn't find our buffer in redis
  //
  // META does not exist or was never entered.  Re-read both meta and data.
  ft = isFileType(fn);
  switch (ft) {
  case HDF5:
    rtn->meta = isH5GetMeta(fn);
    isH5GetData(fn, rtn);
    break;
      
  case RAYONIX:
  case RAYONIX_BS:
    rtn->meta = isRayonixGetMeta(fn);
    isRayonixGetData(fn, rtn);
    break;
      
  case UNKNOWN:
  default:
    fprintf(stderr, "%s: unknown file type '%d' for file %s\n", id, ft, fn);
    pthread_rwlock_unlock(&rtn->buflock);
    free(key);
    return NULL;
  }

  #ifndef IS_IGNORE_REDIS_STORE
  isWriteImageBufToRedis(rtn, rc);
  #endif

  pthread_rwlock_unlock(&rtn->buflock);
  pthread_rwlock_rdlock(&rtn->buflock);

  return rtn;
}
