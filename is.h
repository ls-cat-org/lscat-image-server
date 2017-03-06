#define _GNU_SOURCE
#define __USE_GNU

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <jansson.h>
#include <pwd.h>
#include <gpgme.h>
#include <pwd.h>
#include <pthread.h>
#include <poll.h>
#include <search.h>
#include <hdf5.h>
#include <tiffio.h>
#include <setjmp.h>

#define IS_JOB_QUEUE_LENGTH 1024
#define N_WORKER_THREADS 5
#define ISPADSIZE   128

typedef enum {NOACCESS, READABLE, WRITABLE} image_access_type;

typedef enum {UNKNOWN, BLANK, HDF5, RAYONIX, RAYONIX_BS} image_file_type;

typedef struct isProcessListStruct {
  struct isProcessListStruct *next;
  const char *key;
  int esaf;
  pid_t processID;
  json_t *isAuth;
  int do_not_call;
  pthread_t threads[N_WORKER_THREADS];
} isProcessListType;


    //
    // Expect parameters from parse JSON string from the ISREQESTS list
    //
    //  type:        String   The output type request.  JPEG for now.  TODO: movies, profiles, tarballs, whatever
    //  tag:         String   Identifies (with pid) the connection that needs this image
    //  pid:         String   Identifies the user who is requesting the image
    //  stop:        Boolean  Tells the output method to cease updating image
    //  fn:          String   Our file name.  Either relative or fully qualified OK
    //  frame:       Int      Our frame number
    //  xsize:       Int      Width of output image
    //  ysize:       Int      Height of output image
    //  contrast:    Int      greater than this is black.  -1 means self calibrate, whatever that means
    //  wval:        Int      less than this is white.     -1 means self calibrate, whatever that means
    //  x:           Int      X coord of original image to be left hand edge
    //  y:           Int      Y coord of original image to the the top edge
    //  width:       Int      Width in pixels on original image to process.  -1 means full width
    //  height:      Int      Height in pixels on original image to process.  -1 means full height
    //  labelHeight: Int      Height of the label to be placed at the top of the image (full output jpeg is ysize+labelHeight tall)
    //
typedef struct isRequestStruct {
  json_t *rqst_obj;
  const char *type;
  const char *tag;
  const char *pid;
  int stop;
  const char *fn;
  int frame;
  int xsize;
  int ysize;
  int contrast;
  int wval;
  int x;
  int y;
  int width;
  int height;
  int labelHeight;
} isRequestType;

typedef struct isResponseStruct {
  int dummy;
} isResponseType;

extern void isH5Jpeg(json_t *job);
extern void isRayonixJpeg(json_t *job);
extern void isBlankJpeg(json_t *job);
extern isRequestType *isRequestParser(json_t *);
extern void isRequestDestroyer(isRequestType *a);
extern void isRequestPrint(isRequestType *, FILE *);
extern json_t *decryptIsAuth(gpgme_ctx_t gpg_ctx, const char *isAuth);
extern const char *isFindProcess(const char *pid, int esaf);
extern void isSupervisor(isProcessListType *p);
extern void isProcessDoNotCall( const char *pid, int esaf);
extern const char *isRun(json_t *isAuth_obj, int esaf);
extern void isProcessListInit();
extern image_file_type isFileType(const char *fn);
extern image_access_type isFindFile(const char *fn);
extern int isEsafAllowed(json_t *isAuth, int esaf);
extern void set_json_object_string(const char *cid, json_t *j, const char *key, const char *fmt, ...);
extern void set_json_object_integer(const char *cid, json_t *j, const char *key, int value);
extern void set_json_object_real(const char *cid, json_t *j, const char *key, double value);
