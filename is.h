#define _GNU_SOURCE
#define __USE_GNU

#include <errno.h>
#include <fcntl.h>
#include <hdf5.h>
#include <hiredis/hiredis.h>
#include <jansson.h>
#include <jpeglib.h>
#include <math.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <pwd.h>
#include <pthread.h>
#include <poll.h>
#include <search.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <tiffio.h>
#include <turbojpeg.h>
#include <unistd.h>
#include <zmq.h>

#include "isBitmapFont.h"

// Prefix procedure names with the file name and a space for debug output
#define FILEID __FILE__ " "

// Keep about this many images in memory
#define N_IMAGE_BUFFERS 128

// Each user/esaf combination gets this many threads
#define N_WORKER_THREADS 5

// Keep images in redis for this long
#define IS_REDIS_TTL 300

// There is some overhead for jpegs, this should allow enough overhead for output buffer allocation purposes
#define MIN_JPEG_BUFFER 1024

typedef enum {NOACCESS, READABLE, WRITABLE} image_access_type;

typedef enum {UNKNOWN, BLANK, HDF5, RAYONIX, RAYONIX_BS} image_file_type;

typedef struct isImageBufStruct {
  struct isImageBufStruct *next;        // The next item in our list of buffers
  const char *key;                      // The string that uniquely idenitifies this entry: This is the gid/file path
  pthread_rwlock_t buflock;             // keep our threads from colliding on a specific buffer
  int in_use;                           // Flag to make sure we don't remove this buffer before we can lock it.  Protect with contex mutex
  redisReply *rr;                       // non-NULL when buf points to rr->str
  char *meta_str;                       // String version of the meta object
  json_t *meta;                         // Our meta data
  int buf_size;                         // Size of our buffer in bytes (had better = buf_width * buf_height * buf_depth
  int buf_width;                        // width of the current buffer (may differ from that found in meta)
  int buf_height;                       // height of the current buffer (may differ from that found in meta)
  int buf_depth;                        // depth of the current buffer (may differ from that found in meta)
  void *extra;                          // Whatever the extra stuff this detector requires
  void *bad_pixel_map;                  // If defined assumed uint32_t of same size and shape as buf
  void (*destroy_extra)(void *);        // Function to destroy the extra stuff
  void *buf;                            // Our buffer
} isImageBufType;

typedef struct isImageBufContextStruct {
  isImageBufType *first;                // The first image buffer in our linked list
  const char *key;                      // same as the process list key but accessible to the threads: this is the redis key for the job list
  int n_buffers;                        // The number of buffers in the list (so we know when to remake the hash table
  int max_buffers;                      // Maximum number of buffers allowed in the hash table
  pthread_mutex_t ctxMutex;             //
  struct hsearch_data bufTable;         // Hash table to find the correct buffer quickly
} isImageBufContext_t;

typedef struct isProcessListStruct {
  struct isProcessListStruct *next;     // Linked list of of processes running as a specific user in a specific group
  const char *key;                      // Unique key to identify this user/esaf combination
  int esaf;                             // Our esaf
  pid_t processID;                      // The process id returned to the parent by fork
  json_t *isAuth;                       // Authenticated user name, role, and list of allowed esafs
  int do_not_call;                      // 
} isProcessListType;

extern json_t *isH5GetMeta(const char *fn);
extern json_t *isRayonixGetMeta(const char *fn);
extern void isH5GetData(const char *fn, int frame, isImageBufType *imb);
extern void isRayonixGetData(const char *fn, int frame, isImageBufType *imb);
extern void isBlankJpeg(json_t *job);
extern const char *isFindProcess(const char *pid, int esaf);
extern void isSupervisor(const char *key);
extern void isProcessDoNotCall( const char *pid, int esaf);
const char *isRun(redisContext *rc, redisContext *rcLocal, json_t *isAuth, int esaf);
extern void isProcessListInit();
extern image_file_type isFileType(const char *fn);
extern image_access_type isFindFile(const char *fn);
extern int isEsafAllowed(json_t *isAuth, int esaf);
extern void set_json_object_string(const char *cid, json_t *j, const char *key, const char *fmt, ...);
extern void set_json_object_integer(const char *cid, json_t *j, const char *key, int value);
extern void set_json_object_real(const char *cid, json_t *j, const char *key, double value);
extern void set_json_object_float_array_2d(const char *cid, json_t *j, const char *k, float *v, int rows, int cols);
extern void set_json_object_float_array( const char *cid, json_t *j, const char *key, float *values, int n);
extern isImageBufType *isGetRawImageBuf(isImageBufContext_t *ibctx, redisContext *rc, json_t *job);
extern isImageBufContext_t  *isDataInit(const char *key);
extern int verifyIsAuth( char *isAuth, char *isAuthSig_str);
extern void isDataDestroy(isImageBufContext_t *c);
extern void isWriteImageBufToRedis(isImageBufType *imb, redisContext *rc);
extern isImageBufType *isReduceImage(isImageBufContext_t *ibctx, redisContext *rc, json_t *job);
extern isImageBufType *isGetImageBufFromKey(isImageBufContext_t *ibctx, redisContext *rc, char *key);
extern void isJpeg( isImageBufContext_t *ibctx, redisContext *rc, json_t *job);
