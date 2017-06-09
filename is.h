/*! @file is.h
 *  @brief Header file to be included in all the V2 image server code.
 *  @copyright 2017 Northwestern University All Rights Resesrved
 *  @author Keith Brister
 */
#define _GNU_SOURCE
#define __USE_GNU

#include <errno.h>
#include <fcntl.h>
#include <hdf5.h>
#include <hiredis/hiredis.h>
#include <jansson.h>
#include <jpeglib.h>
#include <math.h>
#include <mcheck.h>
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

#include <assert.h>

// Save our pid so we can autokill stuff later
#define PID_FILE_NAME "/var/run/is.pid"

// Prefix procedure names with the file name and a space for debug output
#define FILEID __FILE__ " "

// Keep about this many images in memory
#define N_IMAGE_BUFFERS 1024

// Each user/esaf combination gets this many threads
#define N_WORKER_THREADS 8

// Keep images in redis for this long
#define IS_REDIS_TTL 300

// There is some overhead for jpegs, this should allow enough overhead for output buffer allocation purposes
#define MIN_JPEG_BUFFER 2048

// Well known address of the image server request dealer
#define PUBLIC_DEALER  "tcp://10.1.253.10:60202"

// Special reply server to handle errors that occur before we can
// route the request to the appropriate process
#define ERR_REP        "inproc://#err_rep"

typedef enum {NOACCESS, READABLE, WRITABLE} image_access_type;

typedef enum {UNKNOWN, BLANK, HDF5, RAYONIX, RAYONIX_BS} image_file_type;

// Filled by isWorker via isData (etc) routines
typedef struct isImageBufStruct {
  struct isImageBufStruct *next;        // The next item in our list of buffers
  const char *key;                      // The string that uniquely idenitifies this entry: This is the gid/file path
  pthread_rwlock_t buflock;             // keep our threads from colliding on a specific buffer
  int in_use;                           // Flag to make sure we don't remove this buffer before we can lock it.  Protect with contex mutex
  redisReply *rr;                       // non-NULL when buf points to rr->str
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

// Managed by isSupervisor (in isWorker.c)
typedef struct isWorkerContextStruct {
  isImageBufType *first;                // The first image buffer in our linked list
  const char *key;                      // same as the process list key but accessible to the threads: this is the redis key for the job list
  int n_buffers;                        // The number of buffers in the list (so we know when to remake the hash table
  int max_buffers;                      // Maximum number of buffers allowed in the hash table
  pthread_mutex_t ctxMutex;             // Lock access to the image buffers
  struct hsearch_data bufTable;         // Hash table to find the correct buffer quickly
  void *zctx;                           // zmq context to transmit data hither and yon
  void *router;                         // zmq socket to talk to our parent process
  void *dealer;                         // zmq socket to talk to our threads
} isWorkerContext_t;

// Owned by isWorker
typedef struct isThreadContextStruct {
  redisContext *rc;                     // redis context opened to local redis server
  void *rep;                            // zmq rep socket to receive whatever data we need to send out
} isThreadContextType;

// Managed by isMain
typedef struct isProcessListStruct {
  struct isProcessListStruct *next;     // Linked list of of processes running as a specific user in a specific group
  const char *key;                      // Unique key to identify this user/esaf combination
  int esaf;                             // Our esaf
  pid_t processID;                      // The process id returned to the parent by fork
  json_t *isAuth;                       // Authenticated user name, role, and list of allowed esafs
  int do_not_call;                      // 
  void *parent_dealer;                  // parent side of parent/child proxy (ipc)
} isProcessListType;

extern json_t *isH5GetMeta(const char *fn);
extern json_t *isRayonixGetMeta(const char *fn);
extern void isH5GetData(const char *fn, int frame, isImageBufType *imb);
extern void isRayonixGetData(const char *fn, int frame, isImageBufType *imb);
extern void isBlankJpeg(json_t *job);
extern isProcessListType *isFindProcess(const char *pid, int esaf);
extern void isSupervisor(const char *key);
extern void isProcessDoNotCall( const char *pid, int esaf);
extern isProcessListType *isRun(void *zctx, redisContext *rc, json_t *isAuth, int esaf);
extern void isProcessListInit();
extern image_file_type isFileType(const char *fn);
extern image_access_type isFindFile(const char *fn);
extern int isEsafAllowed(json_t *isAuth, int esaf);
extern void set_json_object_string(const char *cid, json_t *j, const char *key, const char *fmt, ...);
extern void set_json_object_integer(const char *cid, json_t *j, const char *key, int value);
extern void set_json_object_real(const char *cid, json_t *j, const char *key, double value);
extern void set_json_object_float_array_2d(const char *cid, json_t *j, const char *k, float *v, int rows, int cols);
extern void set_json_object_float_array( const char *cid, json_t *j, const char *key, float *values, int n);
extern isImageBufType *isGetRawImageBuf(isWorkerContext_t *ibctx, redisContext *rc, json_t *job);
extern isWorkerContext_t  *isDataInit(const char *key);
extern int verifyIsAuth( char *isAuth, char *isAuthSig_str);
extern void isDataDestroy(isWorkerContext_t *c);
extern void isWriteImageBufToRedis(isImageBufType *imb, redisContext *rc);
extern isImageBufType *isReduceImage(isWorkerContext_t *ibctx, redisContext *rc, json_t *job);
extern isImageBufType *isGetImageBufFromKey(isWorkerContext_t *ibctx, redisContext *rc, char *key);
extern void isJpeg( isWorkerContext_t *ibctx, isThreadContextType *tcp, json_t *job);
extern void is_zmq_free_fn(void *data, void *hint);
extern void is_zmq_error_reply(zmq_msg_t *msgs, int n_msgs, void *err_dealer, char *fmt, ...);
extern zmq_pollitem_t *isRemakeZMQPollItems(void *parent_router, void *err_rep, void *err_dealer);
extern int isNProcesses();
extern zmq_pollitem_t *isGetZMQPollItems();
extern void isInit();
