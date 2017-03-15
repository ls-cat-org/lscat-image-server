#include "is.h"
#define N_IMAGE_BUFFERS 128


static struct hsearch_data imageBufferTable;
static pthread_mutex_t isImageBufMutex;
static isImageBufType *firstImageBuffer = NULL;
static int n_image_buffers = 0;

void isDataInit() {
  static const char *id = FILEID "isDataInit";
  int err;

  fprintf(stderr, "%s: initializing imageBufferTable with %d entries\n", id, N_IMAGE_BUFFERS);

  errno = 0;
  err = hcreate_r( 2*N_IMAGE_BUFFERS, &imageBufferTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to initialize hash table\n", id);
    exit(-1);
  }
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

isImageBufType *createNewImageBuf(const char *key) {
  static const char *id = FILEID "createNewImageBuf";
  ENTRY item;
  ENTRY *return_item;
  isImageBufType *rtn;
  int i;
  int err;
  int locked;
  isImageBufType *p;
  isImageBufType *last;
  isImageBufType *next;
  // call with isImageBufMutex locked

  fprintf(stderr, "%s: 10\n", id);

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }
  
  rtn->key = strdup(key);
  pthread_rwlock_init(&rtn->buflock, NULL);

  rtn->next = firstImageBuffer;
  firstImageBuffer = rtn;

  item.key  = (char *)rtn->key;
  item.data = rtn;
  errno = 0;
  err = hsearch_r(item, ENTER, &return_item, &imageBufferTable);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to enter item %s: %s\n", id, rtn->key, strerror(errno));
    exit (-1);
  }
  
  if (++n_image_buffers >= N_IMAGE_BUFFERS) {
    // Time to rebuild the hash table
    hdestroy_r(&imageBufferTable);
    errno = 0;
    err = hcreate_r( 2*N_IMAGE_BUFFERS, &imageBufferTable);
    if (err == 0) {
      fprintf(stderr, "%s: Failed to initialize hash table\n", id);
      exit(-1);
    }
    //
    // Reenter the hash table values.  Keep some (say half) of the
    // most recent enties and all of the ones that are currently being
    // used
    //

    last = NULL;
    next = NULL;
    n_image_buffers = 0;
    for(i=0, p=firstImageBuffer; p != NULL; p=next) {
      next = p->next;
      locked = (0 == pthread_rwlock_trywrlock(&p->buflock));
      if (i < N_IMAGE_BUFFERS/2 || locked) {
        n_image_buffers++;
        if (locked) {
          pthread_rwlock_unlock(&p->buflock);
        }

        p->next = last;
        item.key = (char *)p->key;
        item.data = p;
        last = p;
        err = hsearch_r(item, ENTER, &return_item, &imageBufferTable);
        if (err == 0) {
          fprintf(stderr, "%s: Failed to enter item %s\n", id, p->key);
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
  }

  // remember that return value we calculated so long ago?
  return rtn;
}


isImageBufType *isGetImageBuf( redisContext *rc, json_t *job) {
  static const char *id = FILEID "isGetImageBuf";
  const char *fn;
  int frame;
  int frame_strlen;
  ENTRY item;
  ENTRY *return_item;
  isImageBufType *rtn;
  int gid;
  int gid_strlen;
  char *key;
  int key_strlen;
  redisReply *rr;
  json_error_t jerr;
  int found_redis_entry;
  int err;
  image_file_type ft;

  fprintf(stderr, "%s: 010\n", id);

  fn = json_string_value(json_object_get(job, "fn"));
  if (fn == NULL || strlen(fn) == 0) {
    return NULL;
  }

  fprintf(stderr, "%s: 020  fn: %s\n", id, fn);

  gid = getegid();
  if (gid <= 0) {
    fprintf(stderr, "%s: Bad gid %d\n", id, gid);
    return NULL;
  }
  gid_strlen = ((int)log10(gid)) + 1;

  fprintf(stderr, "%s: 30  gid: %d  len: %d\n", id, gid, gid_strlen);

  frame = json_integer_value(json_object_get(job,"frame"));
  if (frame <= 0) {
    frame = 1;
  }

  frame_strlen = ((int)log10(frame)) + 1;

  fprintf(stderr, "%s: 40 frame: %d   len: %d\n", id, frame, frame_strlen);

  //           length of strings plus 2 slashes and one dash  
  key_strlen = strlen(fn) + gid_strlen + frame_strlen +  3;
  key = calloc(1,  key_strlen + 1);
  if (key == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }
  snprintf(key, key_strlen, "%d/%s-%d", gid, fn, frame);
  key[key_strlen] = 0;

  fprintf(stderr, "%s: 50 key: %s  len: %d\n", id, key, key_strlen);
  fflush(stderr);

  rtn = NULL;
  pthread_mutex_lock(&isImageBufMutex);

  fprintf(stderr, "%s: 60 got mutex\n", id);
  fflush(stderr);

  item.key = key;
  err = hsearch_r(item, FIND, &return_item, &imageBufferTable);
  if (err != 0) {
    rtn = return_item->data;
  }
  
  fprintf(stderr, "%s: 70 Found key %s\n", id, rtn->key == NULL ? "<NULL>" : rtn->key);
  fflush(stderr);

  if (rtn == NULL) {
    //
    // Better create and write-lock a new entry
    //
    rtn = createNewImageBuf(key);
    pthread_rwlock_wrlock(&rtn->buflock);
  }
  pthread_mutex_unlock(&isImageBufMutex);

  if (rtn != NULL) {
    free(key);
    return rtn;
  }
  
  //
  // Check the redis store for this data
  //
  // First extend the expiration time for our key
  //
  //
  rr = redisCommand(rc, "EXPIRE %s 300", key);
  if (rr == NULL) {
    fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
    exit (-1);
  }
  
  if (rr->type == REDIS_REPLY_ERROR) {
    fprintf(stderr, "%s: Redis expire command produced an error: %s\n", id, rr->str);
    exit (-1);
  }

  if (rr->type != REDIS_REPLY_INTEGER) {
    fprintf(stderr, "%s: Redis expire did not return an integer, got type %d\n", id, rr->type);
    exit (-1);
  }

  found_redis_entry = rr->integer;
  freeReplyObject(rr);

  if (found_redis_entry == 1) {
    // The key exists and the new expiration time has been set
    //
    rr = redisCommand(rc, "HGET %s META", key);
    if (rr == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }

    if (rr->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis hget command produced an error: %s\n", id, rr->str);
      exit (-1);
    }

    if (rr->type != REDIS_REPLY_STRING) {
      fprintf(stderr, "%s: meta data failed to be a string for key %s\n", id, key);
      exit (-1);
    }

    rtn->meta_str = strdup(rr->str);
    if (rtn->meta_str == NULL) {
      fprintf(stderr, "%s: Out of memory (meta_str)\n", id);
      exit (-1);
    }
    freeReplyObject(rr);
    
    rtn->meta = json_loads(rtn->meta_str, 0, &jerr);
    if (rtn->meta == NULL) {
      fprintf(stderr, "%s: Failed to parse '%s': %s\n", id, rtn->meta_str, jerr.text);
      
      free(key);
      pthread_rwlock_unlock(&rtn->buflock);
      return NULL;
    }
    rtn->extra  = NULL;
    //
    // Got the meta data, now for the actual data
    //
    rtn->rr = redisCommand(rc, "HGET %s DATA", key);
    if (rtn->rr == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    
    if (rtn->rr->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis hget command produced an error: %s\n", id, rtn->rr->str);
      exit (-1);
    }
    
    if (rtn->rr->type != REDIS_REPLY_STRING) {
      fprintf(stderr, "%s: Reading data buffer from redis did not yield a data buffer.  Got type %d\n", id, rtn->rr->type);
      exit (-1);
    }
    rtn->buf_size = rtn->rr->len;
    rtn->buf      = rtn->rr->str;
  } else {
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
      return NULL;
    }
    
    rtn->meta_str = json_dumps(rtn->meta, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    if (rtn->meta_str == NULL) {
      fprintf(stderr, "%s: Could not stringify meta object.  This is not supposed to be possible\n", id);
      exit (-1);
    }

    rr = redisCommand(rc, "HSET %s META %s", key, rtn->meta_str);
    if (rr == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    if (rr->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis hset meta data command produced an error: %s\n", id, rr->str);
      exit (-1);
    }
    freeReplyObject(rr);

    rr = redisCommand(rc, "HSET %s DATA %b", key, rtn->buf, (size_t)rtn->buf_size);
    if (rr == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    if (rr->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis hset data produced an error: %s\n", id, rr->str);
      exit (-1);
    }
    freeReplyObject(rr);

    rr = redisCommand(rc, "EXPIRE %s 300");
    if (rr == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    if (rr->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "%s: Redis expire produced an error: %s\n", id, rr->str);
      exit (-1);
    }
    freeReplyObject(rr);
  }

  free(key);
  pthread_rwlock_unlock(&rtn->buflock);
  return rtn;
}
