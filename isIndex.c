/*! @file isIndex.c
 *  @copyright 2018 by Northwestern University
 *  @author Keith Brister
 *  @brief Support indexing of images
 */
#include "is.h"

json_t *isIndexImages(redisContext *rrc, const char *progressPublisher, const char *tag, const char *f1, const char *f2, const int frame1, const int frame2) {
  static const char *id = FILEID "isIndexImages";
  char *f1_local;               // f1 without dir component
  char *f2_local;               // f2 without dir component
  char *json_output;            // json formated output from called program
  char *json_output_end;        // Place to add json new output 
  char *s_err;                  // stderr output from called program
  char *s_err_end;              // Place to add new stderr output
  char *tmp_dir_template;       // our temporary directory name template
  char *tmp_dir;                // our temporary directory name
  char child_progress[256];     // short progress string to return out-of-band
  int bytes_read;               // generic bytes read from a file descriptor
  int c;                        // returned by fork: 0 = in child, >0 = in parent, -1 = fork error
  int err;                      // returned by waitpid: 0 = still running, > 0 done, -1 error
  int i;                        // loop over poll events
  int json_output_size;         // size of json_output
  int keep_on_truckin;          // flag to keep polling
  int npoll;                    // number of fd for poll to listen for
  int pipeerr[2];               // Our stderr pipes
  int pipein[2];                // Our stdin pipes
  int pipejson[2];              // Our json pipes
  int pipeout[2];               // Our stdout pipes
  int pipeprogress[2];          // Our progress pipes
  int pollstat;                 // result of poll command
  int s_err_size;               // size of s_err
  FILE *shell_script;           // shell script file we are creating (then running)
  int status;                   // status returned by waitpid
  json_error_t jerr;            // error returned when creating json object
  json_t *rtn;                  // our return value
  redisReply *reply;            // returned by redisCommand when we publish our progress
  struct pollfd pollerr;        // poll for stderr 
  struct pollfd polljson;       // poll for json fd
  struct pollfd polllist[5];    // list of fd's we're polling
  struct pollfd pollout;        // poll for stdout
  struct pollfd pollprogress;   // poll for progress


  rtn = NULL;

  /***************************************
   *    Set up pipes to child process    *
   ***************************************/
  pipe2(pipein,       O_CLOEXEC);  // Future use: setup child in anticipation then send command via stdin
  pipe2(pipeout,      O_CLOEXEC);  // We'll be throwing this out by, well, not saving it.
  pipe2(pipeerr,      O_CLOEXEC);  // At some point we'll parse this for errors.  No API developed yet
  pipe2(pipeprogress, 0);          // Progress report to be sent out-of-band to the user
  pipe2(pipejson,     0);          // The result we are looking for as a response to the original request

  char * const index_args[] = {
    "indexing_script.sh",
    NULL
  };

  char * env[] = {
    "BASH_ENV=/usr/local/bin/is_indexing_setup.sh",
    NULL
  };

  c = fork();
  if (c == -1) {
    isLogging_err( "%s: fork 2 failed: %s\n", id, strerror(errno));
    exit (-1);
  }

  if (c == 0) {
    /*************  In Child *****************/  
    //
    // Make use of the O_CLOEXEC option in pipe2, above, so we don't
    // explicitly close the descriptors in the pipes here.
    //
    dup2(pipein[0],    0);      // stdin comes from pipein
    dup2(pipeout[1],   1);      // stdout goes to pipeout
    dup2(pipeerr[1],   2);      // stderr goes to pipeerr

    //close(pipeprogress[0]);     // close unneeded end of pipe
    //close(pipejson[0]);         // close unneeded end of pipe

    //
    // Make tmp directory
    //
    tmp_dir_template = strdup("/pf/tmp/isIndex-XXXXXX");
    tmp_dir = mkdtemp(tmp_dir_template);
    if (tmp_dir == NULL) {
      isLogging_err("%s: failed to create temp directory from template %s: %s", id, tmp_dir_template, strerror(errno));
      exit (-1);
    }

    isLogging_info("%s: Using temp directory %s", id, tmp_dir);

    //
    // Change to tmp directory
    //
    err = chdir(tmp_dir);
    if (err == -1) {
      isLogging_err("%s: failed to change to temp directory %s: %s", id, tmp_dir, strerror(errno));
      exit (-1);
    }

    //
    // Make hard links in tmp directory to data files
    //
    // (Hardlinks aren't working to /pf/esafs but are from
    // /pf/esafs-bu, changed to soft links for now.)
    //
    f1_local = NULL;
    if (f1 && strlen(f1)) {
      f1_local = file_name_component(id, f1);
      err = symlink(f1, f1_local);
      if (err == -1) {
        isLogging_err("%s: failed to link file %s: %s", id, f1, strerror(errno));
        // Should we exit here?
      }
    }

    f2_local = NULL;
    if (f2 && strlen(f2) && strcmp(f1,f2) != 0) {
      f2_local = file_name_component(id, f2);
      err = symlink(f2, f2_local);
      if (err == -1) {
        isLogging_err("%s: failed to link file %s: %s", id, f2, strerror(errno));
        // Should we exit or return here?
      }
    }

    shell_script = fopen("indexing_script.sh", "w");
    if (shell_script == NULL) {
      isLogging_err("%s: could not open indexing_script: %s", id, strerror(errno));
      exit (-1);
    }
    fprintf(shell_script, "#! /bin/bash\n");
    fprintf(shell_script, "rapd.index --json --json-fd %d --progress-fd %d ", pipejson[1], pipeprogress[1]);
    fflush(shell_script);

    // Always need f1 but not always f2
    if (f2==NULL || strlen(f2)==0 || strcmp(f1,f2)==0) {
      if (frame1==0) {
        // Just go for it
        fprintf(shell_script, "%s", f1_local);
      } else {
        // use the requested frames
        fprintf(shell_script, "--hdf5_image_range %d,%d %s", frame1, frame2, f1_local);
      }
    } else {
      // Here f1 and f2 are specified (and different).  We have to
      // assume these files specify a single frame
      fprintf(shell_script, "%s %s", f1_local, f2_local);
    }

    fflush(shell_script);
    fprintf(shell_script, "\nexit 0\n");
    fflush(shell_script);
    fclose(shell_script);
    err = chmod("indexing_script.sh", S_IXUSR | S_IRUSR | S_IWUSR);
    if (err == -1) {
      isLogging_err("%s: could not set mode for script: %s", id, strerror(errno));
      exit (-1);
    }
    
    execve( index_args[0], index_args, env);
  
    // We only get here on failure
    printf("execve 2 failed: %s\n", strerror(errno));
    exit (-1);
  }

  /*************  In Parent *****************/  
  json_output     = NULL;
  json_output_end = NULL;
  json_output_size = 0;
  s_err            = NULL;
  s_err_end        = NULL;
  s_err_size       = 0;

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
  isLogging_info("%s: starting poll loop", id);
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
        keep_on_truckin = 0;
        break;
      }
      if (err == -1) {
        isLogging_info("%s: waitpid 2 failed: %s\n", id, strerror(errno));
        keep_on_truckin = 0;
        break;
      }
      continue;
    }

    for (i=0; i<npoll; i++) {
      // check for errors
      if (polllist[i].revents & (POLLERR | POLLHUP)) {
        isLogging_info("%s: Error or hangup on fd %d\n", id, polllist[i].fd);

        if (polllist[i].fd == pollout.fd) {
          close(polllist[i].fd);
          pollout.fd = -1;
        }

        if (polllist[i].fd == pollerr.fd) {
          close(polllist[i].fd);
          pollerr.fd = -1;
        }

        if (polllist[i].fd == polljson.fd) {
          close(polllist[i].fd);
          polljson.fd = -1;
          isLogging_err("%s: Closing json pipe after reading %d bytes", id, json_output_size);
        }

        if (polllist[i].fd == pollprogress.fd) {
          close(polllist[i].fd);
          pollprogress.fd = -1;
          isLogging_err("%s: Closing progress pipe", id);
        }
      }

      // stdout service
      // 
      // We plan to ignore stdout but we at least need to service
      // the socket so the clild does not hang with a full buffer.
      //
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollout.fd) {
        do {
          char tmp[256];

          bytes_read = read(pollout.fd, tmp, sizeof(tmp));
          if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            isLogging_info("%s: stdout read with error %s", id, strerror(errno));
          }
          //} while(bytes_read > 0);
        } while(0);
      }

      // stderr service
      //
      // How exactly we handle errors is not fully designed at this
      // point in time.  For now we'll pass back the errors in the
      // return value so at least it's all handled in-band.
      //
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollerr.fd) {
        do {
          char tmp[256];

          tmp[0] = 0;
          bytes_read = read(pollerr.fd, tmp, sizeof(tmp));
          if (bytes_read > 0) {
            // new stderr area
            s_err = realloc(s_err, s_err_size + bytes_read);

            // place to add the new stuff
            s_err_end = s_err + s_err_size;

            // add it
            memcpy(s_err_end, tmp, bytes_read);

            // new size of our stderr string
            s_err_size += bytes_read;
          }
          if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            isLogging_info("%s: stderr read with error %s", id, strerror(errno));
          }
          //} while(bytes_read > 0);
        } while(0);
      }

      // progress service
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollprogress.fd) {
        do {
          char tmp[256];
          int i, j;

          bytes_read = read(pollprogress.fd, tmp, sizeof(tmp)-1);
          if (bytes_read > -1) {
            tmp[bytes_read] = 0;
            child_progress[0] = 0;
            for (i=0, j=0; j<bytes_read; j++) {
              if (tmp[j] >= 32 && tmp[j] < 128) {
                child_progress[i++] = tmp[j];
              }
            }
            child_progress[i] = 0;

            if (rrc && progressPublisher) {
              reply = redisCommand(rrc, "PUBLISH %s {\"progress\":\"%s\",\"done\":false,\"tag\":\"%s\"}", progressPublisher, child_progress, tag);
              if (reply == NULL) {
                isLogging_info("%s: redis progress publisher %s returned error %s when publishing \"%*s\"", id, progressPublisher, rrc->errstr, strlen(child_progress), child_progress);
              } else {
                freeReplyObject(reply);
              }
            }
          }
          if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            isLogging_info("%s: progress read error: %s", id, strerror(errno));
          }
          //} while(bytes_read > 0);
        } while(0);
      }

      // json service
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == polljson.fd) {
        do {
          char tmp[256];
          
          tmp[0] = 0;
          bytes_read = read(polljson.fd, tmp, sizeof(tmp));
          if (bytes_read > 0) {
            json_output = realloc(json_output, json_output_size + bytes_read);
            
            // location to start writing new data
            json_output_end = json_output + json_output_size;
            
            // move read data to the json buffer
            memcpy(json_output_end, tmp, bytes_read);
            
            // save the new json_output buffer size
            json_output_size += bytes_read;
          }
          if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            isLogging_info("json read with error %s", strerror(errno));
          }
          //} while(bytes_read > 0);
        } while( 0);
      }
    }
  } while (npoll && keep_on_truckin);
    
  reply = redisCommand(rrc, "PUBLISH %s {\"done\":true,\"tag\":\"%s\"}", progressPublisher, tag);
  if (reply == NULL) {
    isLogging_info("%s: redis progress publisher %s returned error %s", id, progressPublisher, rrc->errstr);
  } else {
    freeReplyObject(reply);
  }

  //
  // Sometimes rapd forgets to send us the json.  Don't bother trying
  // to parse.
  //
  if (json_output_size == 0) {
    rtn = NULL;
  } else {
    rtn = json_loadb(json_output, json_output_size, 0, &jerr);
    if (rtn == NULL ) {
      isLogging_info("%s: json decode error for string '%s': %s line %d  column %d", id, json_output, jerr.text, jerr.line, jerr.column);
    }
  }

  //
  // If we did happen to get something from stderr then we'll add that
  // to the output as it might help us sort things out.
  //
  if (s_err_size > 0) {
    json_t *j_stderr;

    if (rtn == NULL) {
      rtn = json_object();
    }
    j_stderr = json_stringn(s_err, s_err_size);
    free(s_err);
    json_object_set_new(rtn, "stderr", j_stderr);
  }

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
  const char *tag;
  const char *progressPublisher;
  const char *progressAddress;
  int   progressPort;
  redisContext *remote_redis;
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
  tag               = json_string_value(json_object_get(job,  "tag"));
  fn1               = json_string_value(json_object_get(job,  "fn1"));
  fn2               = json_string_value(json_object_get(job,  "fn2"));
  frame1            = json_integer_value(json_object_get(job, "frame1"));
  frame2            = json_integer_value(json_object_get(job, "frame2"));
  progressPublisher = json_string_value(json_object_get(job,  "progressPublisher"));
  progressAddress   = json_string_value(json_object_get(job,  "progressAddress"));
  progressPort      = json_integer_value(json_object_get(job, "progressPort"));
  pthread_mutex_unlock(&wctx->metaMutex);

  //
  // Set up progress reporting if requested.  Not an error if this
  // does not work but log it anyway.
  //
  remote_redis = NULL;
  if (progressPublisher != NULL && progressAddress != NULL && progressPort > 0) {
    remote_redis = redisConnect(progressAddress, progressPort);
    if (remote_redis == NULL || remote_redis->err) {
      if (remote_redis) {
        isLogging_info("%s: Failed to connect to remote redis %s:%d: %s", id, progressAddress, progressPort, remote_redis->err);
      } else {
        isLogging_info("%s: Failed to connect to remote redis %s:%d", id, progressAddress, progressPort);
      }
    }
  }

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
  index = isIndexImages(remote_redis, progressPublisher, tag ? tag : "Tag_Not_Found", fn1, fn2, frame1, frame2);

  redisFree(remote_redis);

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
