/*! @file isIndex.c
 *  @copyright 2018 by Northwestern University
 *  @author Keith Brister
 *  @brief Support indexing of images
 */
#include "is.h"

json_t *isIndexImages(json_t *job, const char *f1, const char *f2, const int frame1, const int frame2) {
  json_t *rtn;

  rtn = NULL;
  return rtn;
}

/** Index diffraction pattern(s)
 **
 ** @param wctx Worker context
 **   @li @c wctx->ctxMutex Keeps worker threads from colliding
 **
 ** @param tcp Thread data
 **   @li @c tcp->rep ZMQ Response socket into return result or error
 **
 ** @param job  Our marching orders
 **   @li @c job->fn1     Our first file name to index
 **   @li @c job->fn2     Our second file name to index
 **   @li @c job->frame1  Frame to use in file fn1
 **   @li @c job->frame2  Frame to use in file fn2
 **
 **   @note: Rayonix files we are expecting frame1 and frame2 to both be 1 and the file names be different
 **          H5 file we are expecting fn1 and fn2 to be the same and the frame numbers to be different
 **
 */
void isIndex(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isIndex";
  const char *fn1;
  const char *fn2;
  int  frame1;
  int  frame2;
  char *job_str;                // stringified version of job
  char *index_str;               // stringified version of meta
  int err;                      // error code from routies that return integers
  json_t *index;                // result object
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // the job message
  zmq_msg_t index_msg;          // the indexing result message to send via zmq

  pthread_mutex_lock(&wctx->metaMutex);
  fn1 = json_string_value(json_object_get(job, "fn1"));
  fn2 = json_string_value(json_object_get(job, "fn2"));
  frame1 = json_integer_value(json_object_get(job, "frame1"));
  frame2 = json_integer_value(json_object_get(job, "frame2"));
  pthread_mutex_unlock(&wctx->metaMutex);

  // Err message part
  zmq_msg_init(&err_msg);

  // Job message part
  job_str = NULL;
  if (job != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    pthread_mutex_unlock(&wctx->metaMutex);
  } else {
    job_str = strdup("");
  }

  // Hey! we already have the job, so lets add it now
  err = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (err != 0) {
    fprintf(stderr, "%s: zmq_msg_init failed (job_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
    pthread_exit (NULL);
  }

  // The actual work is done here
  index = isIndexImages(job, fn1, fn2, frame1, frame2);

  // Indexing result message part
  index_str = NULL;
  if (index != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    index_str = json_dumps(index, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    pthread_mutex_unlock(&wctx->metaMutex);
  } else {
    index_str = strdup("");
  }

  err = zmq_msg_init_data(&index_msg, index_str, strlen(index_str), is_zmq_free_fn, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: zmq_msg_init failed (index_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (index_str)", id);
    pthread_exit (NULL);
  }

  // Send them out
  do {
    // Error Message
    err = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (err == -1) {
      fprintf(stderr, "%s: Could not send empty error frame: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    err = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (err < 0) {
      fprintf(stderr, "%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Meta
    err = zmq_msg_send(&index_msg, tcp->rep, 0);
    if (err == -1) {
      fprintf(stderr, "%s: sending index_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while (0);
}
