/*! @file is.h
 *  @brief Header file to be included in all the V2 image server code.
 *  @copyright 2017 Northwestern University All Rights Resesrved
 *  @author Keith Brister
 */

/** Enable reentrant version of hsearch that is more sensible in a
 ** multi-threaded world.
 */
#define _GNU_SOURCE

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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <tiffio.h>
#include <turbojpeg.h>
#include <unistd.h>
#include <zmq.h>

#include "isBitmapFont.h"

/** make use of assert function to try an catch programming errors
 **/
#define NDEBUG
#include <assert.h>

//! Address of the redis server employed by the LS-CAT Remote Server for session management
#define REMOTE_SERVER_REDIS_ADDRESS "10.1.253.10"

//! TCP Port of the aforementioned redis server
#define REMOTE_SERVER_REDIS_PORT 6379

//! The redis store needs some debugging:  Ignore it for now.
#define IS_IGNORE_REDIS_STORE

//! Save our pid so we can autokill stuff later.
#define PID_FILE_NAME "/var/run/is.pid"

//! Prefix procedure names with the file name and a space for debug output.
#define FILEID __FILE__ " "

//! Keep about this many images in memory.
#define N_IMAGE_BUFFERS 4096

//! Each user/esaf combination gets this many threads.
#define N_WORKER_THREADS 16

//! Keep images in redis for this long.
#define IS_REDIS_TTL 300

//! There is some overhead for jpegs, this should allow enough overhead for output buffer allocation purposes.
#define MIN_JPEG_BUFFER 2048

//! Well known address of the image server request dealer.
#define PUBLIC_DEALER  "tcp://10.1.253.10:60202"

//! Special reply server to handle errors that occur before we can
//! route the request to the appropriate process.
#define ERR_REP        "inproc://#err_rep"

//! Spot sensor sensitivity
//!
//! Pixel value above a std dev to delcare a spot found
#define IS_SPOT_SENSITIVITY 1.5

//! Output image bins for spot finder and, eventually, ice detection
//!
#define IS_OUTPUT_IMAGE_BINS 16

/** The access we've determined by fstat as the uid/gid that will be
 ** trying to read the file.
 */
typedef enum {NOACCESS, READABLE, WRITABLE} image_access_type;

/** The type of image we'll be trying to read.  Based on the file
 ** contents, not by the file extension.
 */
typedef enum {UNKNOWN, BLANK, HDF5, RAYONIX, RAYONIX_BS} image_file_type;

/** Definition of an ice ring                                                                           */
typedef struct ice_ring_struct {
  double high;  //!< Inner part of ice ring in Å
  double low;   //!< Outer part of ice ring in Å
} ice_ring_t;

/** List of ice rings converted into distance from direct beam in mm                                    */
typedef struct ice_ring_list_struct {
  struct ice_ring_list_struct *next;    //!< Next item in our list
  double dist2_low;                     //!< minimum distance in mm to beam center
  double dist2_high;                    //!< maximum distance in mm to beam center
} ice_ring_list_t;

/** Statistics accumulated for an ring of image data                                                    */
typedef struct bin_struct {
  double dist2_low;                     //!< smallest distance^2, in pixels, to beam center
  double dist2_high;                    //!< largest  distance^2, in pixels, to beam center
  ice_ring_list_t *ice_ring_list;       //!< list of ice rings
  double rms;                           //!< rms value for pixels in this bin
  double mean;                          //!< mean value for pixels in this bin
  double sd;                            //!< std deviation for pixels in this bin
  double min;                           //!< minimum pixel value in this bin
  double min_row;                       //!< row number of the minimum pixels in this bin
  double min_col;                       //!< column number of the minimum value in this bin
  double max;                           //!< maximum value in this bin
  double max_row;                       //!< row number of the maximum value in this bin
  double max_col;                       //!< column number of the maximum value in this bin
  double n;                             //!< number of pixels in this bin
  double sum;                           //!< sum of pixel values
  double sum2;                          //!< sum squared of pixel values
} bin_t;

/** Filled by isWorker via isData (etc) routines.                                                */
typedef struct isImageBufStruct {
  struct isImageBufStruct *next;        //!< The next item in our list of buffers
  const char *key;                      //!< The string that uniquely idenitifies this entry: This is the gid/file path
  pthread_rwlock_t buflock;             //!< keep our threads from colliding on a specific buffer
  int in_use;                           //!< Flag to make sure we don't remove this buffer before we can lock it.  Protect with contex mutex
  redisReply *rr;                       //!< non-NULL when buf points to rr->str
  json_t *meta;                         //!< Our meta data
  int buf_size;                         //!< Size of our buffer in bytes (had better = buf_width * buf_height * buf_depth
  int buf_width;                        //!< width of the current buffer (may differ from that found in meta)
  int buf_height;                       //!< height of the current buffer (may differ from that found in meta)
  int buf_depth;                        //!< depth of the current buffer (may differ from that found in meta)
  void *extra;                          //!< Whatever the extra stuff this detector requires
  int frame;                            //!< the frame number
  void *bad_pixel_map;                  //!< If defined assumed uint32_t of same size and shape as buf
  void (*destroy_extra)(void *);        //!< Function to destroy the extra stuff
  void *buf;                            //!< Our buffer
  bin_t bins[IS_OUTPUT_IMAGE_BINS+1];   //!< stats for our spot finder
  double beam_center_x;                 //!< beam_center_x scaled to current image
  double beam_center_y;                 //!< beam_center_x scaled to current image
  double min_dist2;                     //!< square of the minimum possible distance from a pixel to the beam center
  double max_dist2;                     //!< square of the maximum possible distance from a pixel to the beam center
} isImageBufType;

/** Managed by isSupervisor (in isWorker.c)                                                             */
typedef struct isWorkerContextStruct {
  isImageBufType *first;                //!< The first image buffer in our linked list
  const char *key;                      //!< same as the process list key but accessible to the threads: this is the redis key for the job list
  int n_buffers;                        //!< The number of buffers in the list (so we know when to remake the hash table
  int max_buffers;                      //!< Maximum number of buffers allowed in the hash table
  pthread_mutex_t ctxMutex;             //!< Lock access to the image buffers
  pthread_mutex_t metaMutex;             //!< control access to json functions, particularly dumps
  struct hsearch_data bufTable;         //!< Hash table to find the correct buffer quickly
  void *zctx;                           //!< zmq context to transmit data hither and yon
  void *router;                         //!< zmq socket to talk to our parent process
  void *dealer;                         //!< zmq socket to talk to our threads
} isWorkerContext_t;

/** Owned by isWorker                                                                                    */
typedef struct isThreadContextStruct {
  redisContext *rc;                     //!< redis context opened to local redis server
  void *rep;                            //!< zmq rep socket to receive whatever data we need to send out
} isThreadContextType;

/** Managed by isMain                                                                                           */
typedef struct isProcessListStruct {
  struct isProcessListStruct *next;     //!< Linked list of of processes running as a specific user in a specific group
  const char *key;                      //!< Unique key to identify this user/esaf combination
  int esaf;                             //!< Our esaf
  pid_t processID;                      //!< The process id returned to the parent by fork
  json_t *isAuth;                       //!< Authenticated user name, role, and list of allowed esafs
  void *parent_dealer;                  //!< parent side of parent/child proxy (ipc)
} isProcessListType;


extern void destroyImageBuffer(isWorkerContext_t *wctx, isImageBufType *p);
extern json_t *isH5GetMeta(isWorkerContext_t *wctx, const char *fn);
extern json_t *isRayonixGetMeta(isWorkerContext_t *wctx, const char *fn);
extern int isH5GetData(isWorkerContext_t *wctx, const char *fn, isImageBufType **imbp);
extern int isRayonixGetData(isWorkerContext_t *wctx, const char *fn, isImageBufType **imbp);
extern isProcessListType *isFindProcess(const char *pid, int esaf);
extern void isSupervisor(const char *key);
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
extern int get_integer_from_json_object(const char *cid, json_t *j, char *key);
extern double get_double_from_json_object(const char *cid,  const json_t *j, const char *key);
extern isImageBufType *isGetRawImageBuf(isWorkerContext_t *ibctx, redisContext *rc, json_t *job);
extern isWorkerContext_t  *isDataInit(const char *key);
extern int verifyIsAuth( char *isAuth, char *isAuthSig_str);
extern void isDataDestroy(isWorkerContext_t *c);
extern void isWriteImageBufToRedis(isWorkerContext_t *wctx, isImageBufType *imb, redisContext *rc);
extern isImageBufType *isReduceImage(isWorkerContext_t *ibctx, redisContext *rc, json_t *job);
extern isImageBufType *isGetImageBufFromKey(isWorkerContext_t *ibctx, redisContext *rc, char *key);
extern void isJpeg( isWorkerContext_t *ibctx, isThreadContextType *tcp, json_t *job);
extern void isIndex( isWorkerContext_t *ibctx, isThreadContextType *tcp, json_t *job);
extern void is_zmq_free_fn(void *data, void *hint);
extern void is_zmq_error_reply(zmq_msg_t *msgs, int n_msgs, void *err_dealer, char *fmt, ...);
extern zmq_pollitem_t *isRemakeZMQPollItems(void *parent_router, void *err_rep, void *err_dealer);
extern int isNProcesses();
extern zmq_pollitem_t *isGetZMQPollItems();
extern void isInit();
extern void isLogging_init();
extern void isLogging_debug(char *fmt, ...);
extern void isLogging_info(char *fmt, ...);
extern void isLogging_notice(char *fmt, ...);
extern void isLogging_warning(char *fmt, ...);
extern void isLogging_err(char *fmt, ...);
extern void isLogging_crit(char *fmt, ...);
extern void isLogging_alert(char *fmt, ...);
extern void isLogging_emerg(char *fmt, ...);
extern int is_h5_error_handler(hid_t estack_id);
