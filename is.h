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


#define IS_JOB_QUEUE_LENGTH 1024
#define N_WORKER_THREADS 5

typedef struct isProcessListStruct {
  struct isProcessListStruct *next;
  const char *key;
  pid_t processID;
  json_t *isAuth_obj;
  int req_pipe[2];
  FILE *req_fout;       // FILE version of req_pipe[1] so we can use json_dumpf
  char *job_queue[IS_JOB_QUEUE_LENGTH];
  int job_on;
  int job_off;
  int do_not_call;
  pthread_mutex_t job_mutex;
  pthread_cond_t  job_cond;
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

extern isRequestType *isRequestParser(json_t *);
extern void isRequestDestroyer(isRequestType *a);
extern void isRequestPrint(isRequestType *, FILE *);
extern void isBlank(isRequestType *, isResponseType *);
extern void isH5(isRequestType *, isResponseType *);
extern void isRayonix(isRequestType *, isResponseType *);
extern json_t *decryptIsAuth(gpgme_ctx_t gpg_ctx, const char *isAuth);
extern int isHasProcess(const char *pid);
extern void isSupervisor(isProcessListType *p);
extern void isProcessDoNotCall( const char *pid);
extern void isRun(json_t *isAuth_obj, json_t *isRequest);
extern void isProcessListInit();
