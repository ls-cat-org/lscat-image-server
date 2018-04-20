/*! @file isSpots.c
 *  @copyright 2018 by Northwestern University
 *  @author Keith Brister
 *  @brief Find spots in a image
 */
#include "is.h"

/** Count the spots in an image
 **
 ** @param wctx Worker context
 **  @li @c wctx->ctxMutex  Keeps the worker theads in line
 **
 ** @param tcp Thread data
 **   @li @c tcp->rep  ZMQ Response socket into which the throw our response.
 **
 ** @param job  What the user asked us to do
 ** @param pid                 {String}     - Token representing a valid user
 ** @param rqstObj             {Object}     - Description of what is requested
 ** @param rqstObj.esaf        {Inteter}    - experiment id to which this image belongs
 ** @param rqstObj.fn          {String}     - file name
 ** @param rqstObj.frame       {Integer}    - Frame number to return
 ** @param rqstObj.tag         {String}     - ID for us to know what to do with the result
 ** @param rqstObj.type        {String}     - "SPOTS"
 ** @param rqstObj.xsize       {Integer}    - Requested width of resulting jpeg (pixels)
 ** @param rsltCB              {isResultCB} - Callback function when request has been processed
 */
void isSpots(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isSpots";
  const char *fn;                       // file name from job.
  isImageBufType *imb;
  char *job_str;                // stringified version of job
  char *meta_str;               // stringified version of meta
  int err;                      // error code from routies that return integers
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // the job message to send via zmq
  zmq_msg_t meta_msg;           // the metadata to send via zmq
  json_t *jxsize;               // xsize entry in job

  pthread_mutex_lock(&wctx->metaMutex);
  fn = json_string_value(json_object_get(job, "fn"));
  pthread_mutex_unlock(&wctx->metaMutex);

  if (fn == NULL || strlen(fn) == 0) {
    char *tmps;

    pthread_mutex_lock(&wctx->metaMutex);
    tmps = json_dumps(job, JSON_SORT_KEYS | JSON_COMPACT | JSON_INDENT(0));
    pthread_mutex_unlock(&wctx->metaMutex);

    isLogging_err("%s: missing filename for job %s\n", id, tmps);
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Missing filename for job %s", id, tmps);
    free(tmps);

    return;
  }

  // Set a default xsize if none was specified
  jxsize = json_object_get(job, "xsize");
  if (jxsize == NULL ) {
    set_json_object_integer(id, job, "xsize", IS_DEFAULT_SPOT_IMAGE_WIDTH);
  } else {
    json_decref(jxsize);
  }

  // Enforce looking at the full image
  set_json_object_real(id, job, "segcol", 0.0);
  set_json_object_real(id, job, "segrow", 0.0);
  set_json_object_real(id, job, "zoom", 1.0);

  // when isReduceImage returns a buffer it is read locked
  imb = isReduceImage(wctx, tcp->rc, job);
  if (imb == NULL) {
    char *tmps;

    pthread_mutex_lock(&wctx->metaMutex);
    tmps = json_dumps(job, JSON_SORT_KEYS | JSON_COMPACT | JSON_INDENT(0));
    pthread_mutex_unlock(&wctx->metaMutex);

    isLogging_err("%s: missing data for job %s\n", id, tmps);
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Missing data for job %s", id, tmps);
    free(tmps);

    return;
  }

  // Compose messages

  // Err
  zmq_msg_init(&err_msg);

  // Job
  job_str = NULL;
  if (job != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    pthread_mutex_unlock(&wctx->metaMutex);
  }
  if (job_str == NULL) {
    job_str = strdup("");
  }

  err = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (err != 0) {
    isLogging_err("%s: zmq_msg_init failed (job_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
    pthread_exit (NULL);
  }

  // Meta
  meta_str = NULL;
  if (imb->meta != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
     meta_str = json_dumps(imb->meta, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    pthread_mutex_unlock(&wctx->metaMutex);
  }
  if (meta_str == NULL) {
    meta_str = strdup("");
  }

  err = zmq_msg_init_data(&meta_msg, meta_str, strlen(meta_str), is_zmq_free_fn, NULL);
  if (err == -1) {
    isLogging_err("%s: zmq_msg_init failed (meta_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (meta_str)", id);
    pthread_exit (NULL);
  }

  // Send them out
  do {
    // Error Message
    err = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (err == -1) {
      isLogging_err("%s: Could not send empty error frame: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    err = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (err < 0) {
      isLogging_err("%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Meta
    err = zmq_msg_send(&meta_msg, tcp->rep, ZMQ_SNDMORE);
    if (err == -1) {
      isLogging_err("%s: sending meta_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while (0);

  pthread_rwlock_unlock(&imb->buflock);

  pthread_mutex_lock(&wctx->ctxMutex);
  imb->in_use--;

  assert(imb->in_use >= 0);

  pthread_mutex_unlock(&wctx->ctxMutex);
}
