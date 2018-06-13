/*! @file isIndex.c
 *  @copyright 2018 by Northwestern University
 *  @author Keith Brister
 *  @brief Support indexing of images
 */
#include "is.h"

json_t *isIndexImages(json_t *job, const char *f1, const char *f2, const int frame1, const int frame2) {
  static const char *id = FILEID "isIndexImages";
  json_t *rtn;
  int c;
  int pipein[2];
  int pipeout[2];
  int pipeerr[2];
  int pipeprogress[2];
  int pipejson[2];
  int pollstat;
  struct pollfd pollin;
  struct pollfd pollout;
  struct pollfd pollerr;
  struct pollfd pollprogress;
  struct pollfd polljson;
  struct pollfd polllist[5];

  rtn = NULL;

  /***************************************
   *    Set up pipes to child process    *
   ***************************************/
  pipe2(pipein,       O_NONBLOCK | O_CLOEXEC);  // Future use: setup child in anticipation then send command via stdin
  pipe2(pipeout,      O_NONBLOCK | O_CLOEXEC);  // We'll be throwing this out by sending to /dev/null
  pipe2(pipeerr,      O_NONBLOCK | O_CLOEXEC);  // At some point we'll parse this for errors.  No API developed yet
  pipe2(pipeprogress, O_NONBLOCK);              // Progress report to be sent out of band to the user
  pipe2(pipejson,     O_NONBLOCK);              // The result we are looking for as a response to the original request

  char * const index_args[] = {
    "/pf/people/edu/northwestern/k-brister/zz/t.sh",
    NULL
  };

  char * env[] = {
    "BASH_ENV=/usr/local/bin/is_indexing_setup.sh",
    NULL,               // reserved for progress fd
    NULL,               // reserved for json fd
    NULL
  };

  c = fork();
  if (c == -1) {
    printf( "fork 2 failed: %s\n", strerror(errno));
    exit (-1);
  }

  if (c == 0) {
    /*************  In Child *****************/  
    //
    // Make use of the O_CLOEXEC option in pipe2, above, so we don't
    // explicitly close the descriptors in the 5 pipes here.
    //
    dup2(pipein[0],    0);      // stdin comes from pipein
    dup2(pipeout[1],   1);      // stdout goes to pipeout
    dup2(pipeerr[1],   2);      // stderr goes to pipeerr

    close(pipeprogress[0]);     // close unneeded end of pipe
    close(pipejson[0]);         // close unneeded end of pipe

    snprintf(env_progress_fd, sizeof(env_progress_fd)-1, "LSCAT_PROGRESS_FD=%d", pipeprogress[1]);
    env_progress_fd[sizeof(env_progress_fd)-1] = 0;
    env[1]=env_progress_fd;

    snprintf(env_json_fd, sizeof(env_json_fd)-1, "LSCAT_JSON_FD=%d", pipejson[1]);
    env_json_fd[sizeof(env_json_fd)-1] = 0;
    env[2] = env_json_fd;

    execve( index_args[0], index_args, env);
  
    // We only get here on failure
    printf("execve 2 failed: %s\n", strerror(errno));
    exit (-1);
  }
  /*************  In Parent *****************/  

  {
    static char * const fd_names[] = {
      // "stdin",
      "stdout",
      "stderr",
      "json",
      "progress"
    };
    char *json_output, *json_output_end;
    int json_output_size;
    char child_stdout[256];
    char child_stderr[256];
    char child_json[256];
    char child_progress[256];
    int bytes_read;
    int npoll;
    int i;
    int log_stdout, log_stderr, log_json, log_progress;

    
    log_stdout   = open("stdout.log", O_CREAT | O_WRONLY | O_TRUNC );
    log_stderr   = open("stderr.log", O_CREAT | O_WRONLY | O_TRUNC );
    log_json     = open("json.log", O_CREAT | O_WRONLY | O_TRUNC );
    log_progress = open("progress.log", O_CREAT | O_WRONLY | O_TRUNC );

    json_output = NULL;
    json_output_end = NULL;
    json_output_size = 0;

    pollout.fd = pipeout[0];
    pollout.events = POLLIN;

    pollerr.fd = pipeerr[0];
    pollerr.events = POLLIN;

    pollprogress.fd = pipeprogress[0];
    pollprogress.events = POLLIN;

    polljson.fd     = pipejson[0];
    polljson.events = POLLIN;

    close(pipeprogress[1]);       // close unneeded end of pipe
    close(pipejson[1]);           // close unneeded end of pipe

    //
    // Ignore pollin for now
    //
    keep_on_truckin = 1;
    printf( "starting poll loop\n");
    do {
      npoll = 0;;
      if (pollout.fd >= 0) {
        polllist[npoll++] = pollout;
      }
      if (pollerr.fd >= 0) {
        polllist[npoll++] = pollerr;
      }
      if (polljson.fd >= 0) {
        polllist[npoll++] = polljson;
      }
      if (pollprogress.fd >= 0) {
        polllist[npoll++] = pollprogress;
      }

      pollstat = poll( polllist, npoll, 100);
      if (pollstat == -1) {
        printf("poll exited with error: %s\n", strerror(errno));
        exit (-1);
      }

      if (pollstat == 0) {
        //
        // Check to see if our process is still running
        //
        err = waitpid(c, &status, WNOHANG);
        if (err == c) {
          printf("*******  process ended  ********\n");

          //printf("JSON RETURNED\n%*s\n", json_output_size, json_output);
          printf("json_output_size: %d\n", json_output_size);
          keep_on_truckin = 0;
          break;
        }
        if (err == -1) {
          printf("waitpid 2 failed: %s\n", strerror(errno));
          exit (-1);
        }

        continue;
      }

      for (i=0; i<npoll; i++) {
        // check for errors
        if (polllist[i].revents & (POLLERR | POLLHUP)) {
          printf("Error or hangup on fd %d\n", polllist[i].fd);
          if (polllist[i].fd == pollout.fd) {
            close(pollout.fd);
            pollout.fd = -1;
          }

          if (polllist[i].fd == pollerr.fd) {
            close(pollerr.fd);
            pollerr.fd = -1;
          }

          if (polllist[i].fd == polljson.fd) {
            close(polljson.fd);
            polljson.fd = -1;
          }

          if (polllist[i].fd == pollprogress.fd) {
            close(pollprogress.fd);
            pollprogress.fd = -1;
          }
        }

        // stdout service
        if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollout.fd) {
          do {
            child_stdout[0] = 0;
            bytes_read = read(pollout.fd, child_stdout, sizeof(child_stdout));
            if (bytes_read > -1) {
              child_stdout[bytes_read] = 0;
              printf("read %d bytes from stdout: %s\n", bytes_read, child_stdout);
              write(log_stdout, child_stdout, bytes_read);
            }
            if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
              printf("stdout read with error %s\n", strerror(errno));
            }
          } while(bytes_read > 0);
        }

        // stderr service
        if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollerr.fd) {
          do {
            child_stderr[0] = 0;
            bytes_read = read(pollerr.fd, child_stderr, sizeof(child_stderr)-1);
            if (bytes_read > -1) {
              child_stderr[bytes_read] = 0;
              printf("read %d bytes from stderr: %s\n", bytes_read, child_stderr);
              write(log_stderr, child_stderr, bytes_read);
            }
            if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
              printf("stderr read with error %s\n", strerror(errno));
            }
          } while(bytes_read > 0);
        }

        // progress service
        if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollprogress.fd) {
          do {
            child_progress[0] = 0;
            bytes_read = read(pollprogress.fd, child_progress, sizeof(child_progress)-1);
            if (bytes_read > -1) {
              child_progress[bytes_read] = 0;
              printf("read %d bytes from progress pipe: %s\n", bytes_read, child_progress);
              write(log_progress, child_progress, bytes_read);
            }
            if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
              printf("progress read with error %s\n", strerror(errno));
            }
          } while(bytes_read > 0);
        }

        // json service
        if ((polllist[i].revents & POLLIN) && polllist[i].fd == polljson.fd) {
          do {
            char tmp[256];
          
            tmp[0] = 0;
            bytes_read = read(polljson.fd, tmp, sizeof(tmp));
            if (bytes_read > 0) {
              write(log_json, tmp, bytes_read);
              json_output = realloc(json_output, json_output_size + bytes_read);
            
              // location to start writing new data
              json_output_end = json_output + json_output_size;
            
              // move read data to the json buffer
              memcpy(json_output_end, tmp, bytes_read);
            
              // save the new json_output buffer size
              json_output_size += bytes_read;
            }
            if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
              printf("json read with error %s\n", strerror(errno));
            }
          } while(bytes_read > 0);
        }
      }
    } while (keep_on_truckin);

    close(log_stdout);
    close(log_stderr);
    close(log_progress);
    close(log_json);
  



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
    isLogging_err("%s: zmq_msg_init failed (job_str): %s\n", id, zmq_strerror(errno));
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
    isLogging_err("%s: zmq_msg_init failed (index_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (index_str)", id);
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
    err = zmq_msg_send(&index_msg, tcp->rep, 0);
    if (err == -1) {
      isLogging_err("%s: sending index_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while (0);
}
