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
void destroyImageBuffer(isWorkerContext_t *wctx, isImageBufType *p) {
  static const char *id = FILEID "destroyImageBuffer";
  (void)id;

  isLogging_info("%s: destroying image buffer %s\n", id, p->key);

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
    pthread_mutex_lock(&wctx->metaMutex);
    json_decref(p->meta);
    pthread_mutex_unlock(&wctx->metaMutex);
    p->meta = NULL;
  }
  free(p);
  isLogging_info("%s: done\n", id);
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
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->key = strdup(key);
  if (rtn->key == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->n_buffers   = 0;
  rtn->max_buffers = 2 * N_IMAGE_BUFFERS;
  rtn->first     = NULL;

  rtn->zctx = zmq_ctx_new();
  rtn->router = zmq_socket(rtn->zctx, ZMQ_ROUTER);
  if (rtn->router == NULL) {
    isLogging_crit("%s: Could not create router socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->router, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_crit("%s: Could not set RCVHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->router, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_crit("%s: Could not set SNDHWM for router: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(router_endpoint, sizeof(router_endpoint)-1, "ipc://@%s", key);
  router_endpoint[sizeof(router_endpoint)-1] = 0;

  err = zmq_connect(rtn->router, router_endpoint);
  if (err == -1) {
    isLogging_crit("%s: failed to connect to endpoint %s: %s\n", id, router_endpoint, strerror(errno));
    exit (-1);
  }
                    
  rtn->dealer = zmq_socket(rtn->zctx, ZMQ_DEALER);
  if (rtn->dealer == NULL) {
    isLogging_crit("%s: Could not create dealer socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->dealer, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_crit("%s: Could not set RCVHWM for dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->dealer, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_crit("%s: Could not set SNDHWM for dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "inproc://#%s", key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;
  err = zmq_bind(rtn->dealer, dealer_endpoint);
  if (err == -1) {
    isLogging_crit("%s: Could not bind dealer endpoint %s: %s\n", id, dealer_endpoint, strerror(errno));
    exit (-1);
  }
  
  pthread_mutexattr_init(&matt);
  pthread_mutexattr_settype(&matt, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutexattr_setpshared(&matt, PTHREAD_PROCESS_SHARED);
  pthread_mutex_init(&rtn->ctxMutex, &matt);
  pthread_mutexattr_destroy(&matt);

  pthread_mutex_init(&rtn->metaMutex, NULL);

  err = hcreate_r( 2*N_IMAGE_BUFFERS, &rtn->bufTable);
  if (err == 0) {
    isLogging_crit("%s: Failed to initialize hash table: %s\n", id, strerror(errno));
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

  isLogging_info("%s: start\n", id);
  //
  // We are called from isSupervisor after all the threads have been
  // joined: there is no danger of collision and, hence, no need to
  // lock anything.
  //

  hdestroy_r(&c->bufTable);

  next = NULL;
  for (p=c->first; p!=NULL; p=next) {
    next = p->next;     // need to save next since p is going away.
    destroyImageBuffer(c, p);
  }
  c->n_buffers = 0;
  c->first = NULL;
  pthread_mutex_destroy(&c->ctxMutex);
  pthread_mutex_destroy(&c->metaMutex);
  free((char *)c->key);
  free(c);
  isLogging_info("%s: Done\n", id);
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
    isLogging_crit("%s: Could not open file %s: %s\n", id, fn, strerror(errno));
    return NOACCESS;
  }

  errno = 0;
  err = fstat(fd, &buf);
  stat_errno = errno;
  close(fd);                    // let's not be leaking file descriptors

  if (err != 0) {
    isLogging_crit("%s: Could not find file '%s': %s\n", id, fn, strerror(stat_errno));
    return NOACCESS;
  }
  
  if (!S_ISREG(buf.st_mode)) {
    isLogging_crit("%s: %s is not a regular file\n", id, fn);
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
  int fn_len;

  errno = 0;
  fd = open(fn, O_RDONLY);
  if (fd == -1) {
    isLogging_crit("%s: Could not open file '%s'  uid: %d  gid: %d  error: %s\n", id, fn, geteuid(), getegid(), strerror(errno));
    return LSCAT_IMG_UNKNOWN;
  }

  nbytes = read(fd, (char *)&buf4, 4);
  close(fd);
  if (nbytes != 4) {
    isLogging_crit("%s: Could not read 4 bytes from file '%s'\n", id, fn);
    return LSCAT_IMG_UNKNOWN;
  }

  // For most file types, we are content to save time
  // and trust that the file extension is not misleading.
  // LS-CAT ultimately controls what data lands and where
  // it lands within its own compute/storage cluster.
  fn_len = (int)strlen(fn);
  if (strcmp(&(fn[(fn_len-4)]), ".tif") == 0
      || strcmp(&(fn[(fn_len-5)]), ".tiff") == 0) {
    return LSCAT_IMG_GENERIC_TIFF;
  } else if (strcmp(&(fn[fn_len-4]), ".cbf") == 0) {
    return LSCAT_IMG_GENERIC_CBF;
  } else if (strcmp(&(fn[fn_len-3]), ".h5") == 0) {
    return LSCAT_IMG_NEXUSV1_HDF5;
  } else if (strcmp(&(fn[fn_len-5]), ".mccd") == 0) {
    return LSCAT_IMG_RAYONIX;
  } 

  /*
    If there is no universally-recognizable file extension,
    we are most likely using Keith's old naming scheme for
    marCCD images.
    
    Check 2 variants of the TIFF magic number:
    49 49 2A 00 - "Intel byte ordering" (little endian)
    4D 4D 2A 00 - "Motorola byte ordering" (big endian)
    ... And check both byte orderings of each number.
  */
  if (buf4 == 0x002a4949 || buf4 == 0x49492a00
      || buf4 == 0x002a4d4d || buf4 == 0x4d4d2a00) {
    return LSCAT_IMG_RAYONIX;
  }

  /*
    I assume H5 is computationally cheap enough to inspect.
    I don't know why Keith felt the need to inspect the file contents,
    as data directories can only contain images conforming to one of
    our supported types.
  */
  herr = H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
  if (herr < 0) {
    isLogging_crit("%s: Could not turn off HDF5 error reporting\n", id);
  }

  ish5 = H5Fis_hdf5(fn);

  herr = H5Eset_auto2(H5E_DEFAULT, (H5E_auto2_t) is_h5_error_handler, stderr);
  if (herr < 0) {
    isLogging_crit("%s: Could not turn back on HDF5 error reporting\n", id);
  }
  
  if (ish5 > 0) {
    return LSCAT_IMG_NEXUSV1_HDF5;
  }

  isLogging_crit("%s: Unknown file type '%s' buf4 = %x08\n", id, fn, buf4);
  return LSCAT_IMG_UNKNOWN;
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
    isLogging_crit("%s: Out of memory\n", id);
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
    isLogging_crit("%s: Failed to enter item %s: %s\n", id, rtn->key, strerror(errno));
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
  isLogging_info("%s: Rebuilding hash table\n", id);

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

  isLogging_info("%s: ************ Rebuilding hash table. i=%d, max_buffers=%d\n", id, i, wctx->max_buffers);

  hdestroy_r(&wctx->bufTable);
  errno = 0;
  err = hcreate_r( wctx->max_buffers, &wctx->bufTable);
  if (err == 0) {
    isLogging_crit("%s: Failed to initialize hash table\n", id);
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
      destroyImageBuffer(wctx, p);
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
      isLogging_crit("%s: Failed to enter item %s\n", id, p->key);
      exit (-1);
    }
  }    

  // remember that return value we calculated so long ago?
  //isLogging_info("%s: returning after rebuilding table.  key: %s\n", id, rtn->key);
  return rtn;
}

/**
 * Look to see if the data are already available to us from the image
 * buffer hash table. We will wait for the data to appear if another
 * process already is processing this.
 *
 *  When the data is availabe we'll return a read locked buffer.
 *  Otherwise, we'll return a write locked buffer w/ blank data.
 */
isImageBufType *isGetImageBufFromKey(isWorkerContext_t *wctx, char *key) {
  static const char *id = FILEID "isGetImageBufFromKey";
  isImageBufType *rtn = NULL; // This is our return value
  ENTRY item;
  ENTRY *return_item = NULL;
  int err = 0;

  pthread_mutex_lock(&wctx->ctxMutex);

  item.key  = key;
  item.data = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &return_item, &wctx->bufTable);
  if (err != 0) {
    rtn = return_item->data;
  }
  
  if (rtn != NULL) {
    // Somebody else already prepared the data for us, put a read lock
    // on it.
    isLogging_debug("%s: found existing copy of image buf and metadata for %s.\n", id, key);
    assert(rtn->in_use >= 0);
    rtn->in_use++; // flag to keep our buffer in scope while we need it
    pthread_mutex_unlock(&wctx->ctxMutex);
    pthread_rwlock_rdlock(&rtn->buflock);
  } else {  
    //
    // Create a new entry then read some data into it.  We still have
    // bufMutex write locked: This is used to avoid contention with
    // other writers as well as potential readers.  We'll grab the read
    // lock on the buffer before releasing the context mutex.
    //
    isLogging_debug("%s: image buf and metadata for %s do not exist yet, this thread is about to prepare them.\n", id, key);
    rtn = createNewImageBuf(wctx, key);
    pthread_mutex_unlock(&wctx->ctxMutex); // We can now allow access to the other buffers
  }
  return rtn;
}

/** Get the unreduced image
 */
isImageBufType *isGetRawImageBuf(isWorkerContext_t *wctx, json_t *job) {
  static const char *id = FILEID "isGetRawImageBuf";
  const char *fn = NULL;
  int frame      = -1;
  int frame_strlen = 0;
  isImageBufType *rtn = NULL;
  int gid        = -1;
  int gid_strlen = 0;
  char *key      = NULL;
  int key_strlen = 0;
  image_file_type ft;
  int err        = -1;
  json_t* meta   = NULL;

  pthread_mutex_lock(&wctx->metaMutex);
  fn = json_string_value(json_object_get(job, "fn"));
  frame = json_integer_value(json_object_get(job,"frame"));
  pthread_mutex_unlock(&wctx->metaMutex);
  if (fn == NULL || strlen(fn) == 0) {
    return NULL;
  }
  
  if (frame <= 0) {
    frame = 1;
  }

  gid = getegid();
  if (gid < 0) {
    isLogging_crit("%s: Bad gid %d\n", id, gid);
    return NULL;
  }
  gid_strlen = ((int)log10(gid)) + 1;
  frame_strlen = ((int)log10(frame)) + 1;

  //           length of strings plus 2 slashes and one dash  
  key_strlen = strlen(fn) + gid_strlen + frame_strlen +  3;
  key = calloc(1,  key_strlen + 1);
  if (key == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }
  snprintf(key, key_strlen, "%d:%s-%d", gid, fn, frame);
  key[key_strlen] = 0;

  // Get the buffer and fill it if it's in redis already
  //
  // Buffer is read locked if it exists, write locked if it does not
  //
  // isLogging_info("%s: about to get image buffer from key %s\n", id, key);

  rtn = isGetImageBufFromKey(wctx, key);
  rtn->frame = frame;
  if (rtn->buf != NULL) {
    isLogging_crit("%s: Found buffer for key %s\n", id, key);
    free(key);
    return rtn;
  }
  free(key);

  // The image has not been processed for us yet.
  // We need to fetch both the metadata and data.
  // Do it as atomically as possible to avoid holding a lock forever.
  err = -1;
  ft = isFileType(fn);
  switch (ft) {
  case LSCAT_IMG_NEXUSV1_HDF5:
    meta = isH5GetMeta(fn);
    if (!meta) {
      break;
    }
    pthread_mutex_lock(&wctx->metaMutex);
    rtn->meta = meta;
    pthread_mutex_unlock(&wctx->metaMutex);
    err = isH5GetData(fn, rtn);
    break;
  case LSCAT_IMG_GENERIC_CBF:
    meta = isCbfGetMeta(fn);  
    if (!meta) {
      break;
    }
    pthread_mutex_lock(&wctx->metaMutex);
    rtn->meta = meta;
    pthread_mutex_unlock(&wctx->metaMutex);
    err = isCbfGetData(fn, rtn);
    break;
  case LSCAT_IMG_GENERIC_TIFF:
    meta = isTiffGetMeta(fn);
    if (!meta) {
      break;
    }
    pthread_mutex_lock(&wctx->metaMutex);
    rtn->meta = meta;
    pthread_mutex_unlock(&wctx->metaMutex);
    err = isTiffGetData(fn, rtn);
    break;
  case LSCAT_IMG_RAYONIX:
  case LSCAT_IMG_RAYONIX_BS:
    meta = isRayonixGetMeta(fn);
    if (!meta) {
      break;
    }
    pthread_mutex_lock(&wctx->metaMutex);
    rtn->meta = meta;
    pthread_mutex_unlock(&wctx->metaMutex);
    err = isRayonixGetData(fn, rtn);
    break;
      
  case LSCAT_IMG_UNKNOWN:
  default:
    isLogging_crit("%s: unknown file type '%d' for file %s\n", id, ft, fn);
    err = -1;
  }

  if (err != 0) {
    pthread_rwlock_unlock(&rtn->buflock);
    return NULL;
  }

  pthread_rwlock_unlock(&rtn->buflock);
  pthread_rwlock_rdlock(&rtn->buflock);

  return rtn;
}
