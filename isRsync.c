/*! @file isRsync.c
 *  @copyright 2020 by Northwestern University
 *  @author Keith Brister
 *  @brief Support for Rsync
 */
#include "is.h"

/**
 ** Return size of local directory
 **
 ** @param wctx Work context
 **  @li @c  wctx-ctxMutex   Prevents conflicts
 **
 ** @param tcp Tread data
 **   @li @c tcp->rep  ZMQ Response socket into which to throw our response
 **
 **
 ** @param job
 */
void isRsyncLocalDirStats(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isRsyncLocalSize";
  const char *localDirName;             // our name to look up
  json_t *rtn_json;                     // Object to return
  char *rtn_str;                        // string version of rtn_json
  char *job_str;                        // string version of job
  zmq_msg_t err_msg;                    // Place holder for error return
  zmq_msg_t job_msg;                    // send back the object that we are operating on
  zmq_msg_t rtn_msg;                    // message to send back to requester
  FTS *top_ftsp;                        // From fts_open
  FTSENT *ftsp;                         // structure to walk the directory tree
  FTSENT *ftsep;                        // pointer to linked list of directory contents
  const char *dirList[2];               // fodder for fts_open
  uint64 nbytes;                           // sum of file sizes
  uint64 nfiles;                           // number of files found
  int ndirs;                            // number of directories found
  int ncirculars;                       // number of directories that lead to infinite loops
  int nsymlinks;                        // number of symbolic links
  int nbadsymlinks;                     // number of bad symbolic links
  int zerr;                             // error code from zmq functions

  nbytes       = 0;
  nfiles       = 0;
  ndirs        = 0;
  ncirculars   = 0;
  nsymlinks    = 0;
  nbadsymlinks = 0;

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

  zerr = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: job_msg initialization failed: %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize job_msg (job_str)", id);
    pthread_exit(NULL);
  }

  // And our result object
  rtn_json = json_object();


  localDirName = json_string_value(json_object_get(job, "localDirName"));
  dirList[0] = localDirName;
  dirList[1] = NULL;

  top_ftsp = fts_open((char **)dirList, FTS_PHYSICAL | FTS_NOCHDIR, NULL);
  if (top_ftsp != NULL) {
    while((ftsp=fts_read(top_ftsp)) != NULL) {
      ftsep = fts_children(top_ftsp, 0);

      if (errno != 0) {
        isLogging_err("%s: fts_children failed: %s\n", id, strerror(errno));
        // And now what?
      }

      while(ftsep != NULL) {
        switch (ftsep->fts_info) {
        case FTS_F:
          nfiles++;
          nbytes += ftsep->fts_statp->st_size;
          break;

        case FTS_D:
          ndirs++;
          break;

        case FTS_DC:
          ncirculars++;
          //
          // TODO: keep track of the bad directory names
          //
          break;

        case FTS_SL:
          nsymlinks++;
          break;

        case FTS_SLNONE:
          nbadsymlinks++;
          break;

        }

        ftsep = ftsep->fts_link;
      }
    }
    fts_close(top_ftsp);
  }

  json_object_set_new(rtn_json, "localDirName", json_string(localDirName));
  json_object_set_new(rtn_json, "localDirSize", json_integer(nbytes));
  json_object_set_new(rtn_json, "nDirs",        json_integer(ndirs));
  json_object_set_new(rtn_json, "nFiles",       json_integer(nfiles));
  json_object_set_new(rtn_json, "nCircDirs",    json_integer(ncirculars));
  json_object_set_new(rtn_json, "nSymLinks",    json_integer(nsymlinks));
  json_object_set_new(rtn_json, "nBadSymLinks", json_integer(nbadsymlinks));
  
  pthread_mutex_lock(&wctx->metaMutex);
  rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  pthread_mutex_unlock(&wctx->metaMutex);
    
  json_decref(rtn_json);

  zerr = zmq_msg_init_data(&rtn_msg, rtn_str, strlen(rtn_str), is_zmq_free_fn, NULL);
    
  // Send them out
  do {
    if (zerr == -1) {
      isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
      is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
      pthread_exit(NULL);
      break;
    }

    // Error Message
    zerr = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send empty errore: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    zerr = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr < 0) {
      isLogging_err("%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Meta
    zerr = zmq_msg_send(&rtn_msg, tcp->rep, 0);
    if (zerr == -1) {
      isLogging_err("%s: sending results failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while (0);

}


/**
 ** Test for valid host name
 **
 ** @param wctx Worker context
 **   @li @c wctx->ctxMutex  Keeps the worker threads in line
 **
 ** @param tcp Thread data
 **   @li @c tcp->rep  ZMQ Response socket into which to throw our response
 **
 ** @param job
 */

void isRsyncHostTest(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isRsyncHostTest";
  const char *hostName;         // host name to look up
  struct addrinfo *ais;
  int zerr;
  int get_addr_err;

  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // Echo our input object
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *job_str;                // return job info
  char *rtn_str;                // string version of our object

  // Todo: add regexp tests
  //
  hostName = json_string_value(json_object_get(job, "remoteHostName"));

  isLogging_info("%s: request to check host name %s", id, hostName);

  //
  // Really we just send a blank error message: actual errors are
  // signaled by is_zmq_error_reply
  //
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

  zerr = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: job_msg initialization failed: %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize job_msg (job_str)", id);
    pthread_exit(NULL);
  }

  rtn_json = json_object();

  ais = NULL;
  get_addr_err = getaddrinfo(hostName, NULL, NULL, &ais);
  if (ais) {
    freeaddrinfo(ais);
  }

  isLogging_info("%s: host %s is %s: %s", id, hostName, get_addr_err==0 ? "OK" : "Not OK", gai_strerror(get_addr_err));

  //
  // set hostOK and return if the host is not OK
  //
  json_object_set_new(rtn_json, "hostOK", json_boolean(get_addr_err==0));

  if (get_addr_err) {
    json_object_set_new(rtn_json, "hostMsg", json_string(gai_strerror(get_addr_err)));
  }

  do {
    //
    // Send blank error message
    //
    zerr = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send empty error frame: %s\n", id, zmq_strerror(errno));
      break;
    }
    
    //
    // Send our input object
    //
    zerr = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send input object: %s\n", id, zmq_strerror(errno));
      break;
    }
    
    //
    // Send our test result
    //
    pthread_mutex_lock(&wctx->metaMutex);
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    pthread_mutex_unlock(&wctx->metaMutex);
    
    json_decref(rtn_json);
    
    zerr = zmq_msg_init_data(&rtn_msg, rtn_str, strlen(rtn_str), is_zmq_free_fn, NULL);
    if (zerr == -1) {
      isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
      is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
      pthread_exit(NULL);
      break;
    }
    
    zerr = zmq_msg_send(&rtn_msg, tcp->rep, 0);
    if (zerr == -1) {
      isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while (0);
  
  return;
}

/**
 ** Test for valid connection
 **
 ** @param wctx Worker context
 **   @li @c wctx->ctxMutex  Keeps the worker threads in line
 **
 ** @param tcp Thread data
 **   *li *c tcp->rep  ZMQ Response socket into which to throw our response
 **
 ** @param job
 */

void isRsyncConnectionTest(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isRsyncConnectionTest";
  const char *hostName;         // host name to look up
  const char *userName;
  //  const char *localDir;
  const char *destDir;
  char userAtHost[256];
  char *dfCmd;
  int dfCmdSize;
  int pollstat;                 // result of poll command
  int npoll;                    // number of fd for poll to listen for
  int keep_on_truckin;          // flag to keep polling
  int status;                   // status returned by waitpid
  int bytes_read;               // generic bytes read from a file descriptor
  int i;                        // loop over poll events
  char *s_err;                  // stderr output from called program
  char *s_err_end;              // Place to add new stderr output
  int s_err_size;               // size of s_err
  char *s_out;                  // stdout output from called program
  char *s_out_end;              // Place to add new stdout output
  int s_out_size;               // size of s_out
  int err;
  int c;                        // returned by fork: 0 = in child, >0 = in parent, -1 = fork error
  int pipeerr[2];               // Our stderr pipes
  int pipeout[2];               // Our stdout pipes
  struct pollfd pollerr;        // poll for stderr 
  struct pollfd pollout;        // poll for stdout
  struct pollfd polllist[2];    // list of fd's we're polling

  int zerr;
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // send back the object that we are operating on
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *rtn_str;                // string version of our object
  char *job_str;                // string version of job

  // Initialzaition
  s_err            = NULL;
  s_err_end        = NULL;
  s_err_size       = 0;
  s_out            = NULL;
  s_out_end        = NULL;

  s_out_size       = 0;


  // Todo: add regexp tests
  //
  hostName = json_string_value(json_object_get(job, "remoteHostName"));
  userName = json_string_value(json_object_get(job, "remoteUserName"));
  destDir  = json_string_value(json_object_get(job, "remoteDirName"));
  //  localDir = json_string_value(json_object_get(job, "localDirName"));

  snprintf(userAtHost, sizeof(userAtHost)-1, "%s@%s", userName, hostName);
  userAtHost[sizeof(userAtHost)-1] = 0;

  dfCmdSize = 2*strlen(destDir) + 65;
  dfCmd = calloc(dfCmdSize, 1);
  if (dfCmd == NULL) {
    isLogging_crit("%s: Out of memory (isRsync dfCmd)", id);
    exit(-1);
  }

  snprintf(dfCmd, dfCmdSize-1, "sh -c \"mkdir -p %s && df -h %s\"", destDir, destDir);
  dfCmd[dfCmdSize-1] = 0;

  isLogging_info("%s: request to check connection to %s", id, hostName);

  //
  // Really we just send a blank error message: actual errors are
  // signaled by is_zmq_error_reply
  //
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

  zerr = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
    pthread_exit(NULL);
  }

  rtn_json = json_object();

  //
  // Connection test
  //
  /***************************************
   *    Set up pipes to child process    *
   ***************************************/
  pipe2(pipeout,      O_CLOEXEC);  // Returned to user to see how much space they have
  pipe2(pipeerr,      O_CLOEXEC);  // Returned to user to debug errors

  c = fork();
  if (c == -1) {
    isLogging_err( "%s: fork 2 failed: %s\n", id, strerror(errno));
    exit (-1);
  }

  if (c == 0) {
    //
    // In child
    // ========
    //
    char *envp[] = {
      NULL                                // -- Required NULL at end of list
    };
    char *argv[] = {
      "ssh",                              // 0
      "-v",                               // 1
      "-o",                               // 2
      "StrictHostKeyChecking=no",         // 3
      "-o",                               // 4
      "PasswordAuthentication=no",        // 5
      "-o",                               // 6
      "KbdInteractiveDevices=none",       // 7
      "-o",                               // 8
      "ConnectTimeout=5",                 // 9
      NULL,                               // 10 userAtHost
      NULL,                               // 11 dfCmd
      NULL                                // -- Required NULL end of list
    };

    argv[10] = userAtHost;
    argv[11] = dfCmd;

    //
    // Make use of the O_CLOEXEC option in pipe2, above, so we don't
    // explicitly close the descriptors in the pipes here.
    //
    dup2(pipeout[1],   1);      // stdout goes to pipeout
    dup2(pipeerr[1],   2);      // stderr goes to pipeerr

    execve( "/usr/bin/ssh", argv, envp);


    // We only get here on failure
    isLogging_err("%s: execve 2 failed: %s\n", id, strerror(errno));
    exit (-1);
  }

  //
  // In parent
  //

  if (dfCmd) {
    //
    // TODO: put all dfCmd code in child and all other child only
    // variables for that matter.
    //
    free(dfCmd);
  }


  pollout.fd = pipeout[0];
  pollout.events = POLLIN;

  pollerr.fd = pipeerr[0];
  pollerr.events = POLLIN;

  keep_on_truckin = 1;
  isLogging_info("%s: starting poll loop", id);

  //
  // Poll loop
  do {
    npoll = 0;;
    if (pollout.fd >= 0) {
      polllist[npoll++] = pollout;
    }
    if (pollerr.fd >= 0) {
      polllist[npoll++] = pollerr;
    }

    pollstat = poll( polllist, npoll, 100);
    if (pollstat == -1) {
      isLogging_err("%s (parent) poll exited with error: %s\n", id, strerror(errno));
      exit (-1);
    }

    if (pollstat == 0) {
      //
      // Check to see if our process is still running
      //
      err = waitpid(c, &status, WNOHANG);
      if (err == c) {
        if (WIFEXITED(status)) {
          set_json_object_integer(id, rtn_json, "status", WEXITSTATUS(status));
        }

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
      }

      // stdout service
      // 
      // This should be the output from the df command for the
      // directory the user is trying to send stuff to.  This should
      // be useful to inform the user that they need more disk space.
      //
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollout.fd) {
        do {
          char tmp[256];

          tmp[0] = 0;
          bytes_read = read(pollout.fd, tmp, sizeof(tmp));
          if (bytes_read > 0) {
            // new stderr area
            s_out = realloc(s_out, s_out_size + bytes_read);

            isLogging_info("%s: %d bytes of stdout \"%.*s\"", id, bytes_read, bytes_read, tmp);

            // place to add the new stuff
            s_out_end = s_out + s_out_size;

            // add it
            memcpy(s_out_end, tmp, bytes_read);

            // new size of our stderr string
            s_out_size += bytes_read;
          }
          if (bytes_read == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            isLogging_info("%s: stdout read with error %s", id, strerror(errno));
          }
        } while(0);
      }

      // stderr service
      //
      // The test command should generate a lot of chatter useful to
      // debug connections (or to verify that things should be
      // working.
      //
      if ((polllist[i].revents & POLLIN) && polllist[i].fd == pollerr.fd) {
        do {
          char tmp[256];

          tmp[0] = 0;
          bytes_read = read(pollerr.fd, tmp, sizeof(tmp));
          if (bytes_read > 0) {

            isLogging_info("%s: %d bytes of stderr \"%.*s\"", id, bytes_read, bytes_read, tmp);

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
        } while(0);
      }
    }
  } while (npoll && keep_on_truckin);

  //
  // If the connection succeeds we should have the df -h output for the destination directory
  //
  if (s_out_size > 0) {
    json_t *j_stdout;

    j_stdout = json_stringn(s_out, s_out_size);
    free(s_out);
    json_object_set_new(rtn_json, "stdout", j_stdout);
  }


  //
  // This returns all the gory details of the ssh connection.  We can use this to debug the connection if things aren't working.
  //
  if (s_err_size > 0) {
    json_t *j_stderr;

    j_stderr = json_stringn(s_err, s_err_size);
    free(s_err);
    json_object_set_new(rtn_json, "stderr", j_stderr);
  }

  // Return message
  rtn_str = NULL;
  if (rtn_json != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    json_decref(rtn_json);
    pthread_mutex_unlock(&wctx->metaMutex);
  } else {
    rtn_str = strdup("");
  }

  zerr = zmq_msg_init_data(&rtn_msg, rtn_str, strlen(rtn_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: zmq_msg_init failed (rtn_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (rtn_str)", id);
    pthread_exit (NULL);
  }
  
  do {
    // Error Message
    zerr = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send rsync test: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    zerr = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr < 0) {
      isLogging_err("%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Results
    zerr = zmq_msg_send(&rtn_msg, tcp->rep, 0);
    if (zerr < 0) {
      isLogging_err("%s: sending rtn_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while(0);
}


/**
 ** Test for valid connection
 **
 ** @param wctx Worker context
 **   @li @c wctx->ctxMutex  Keeps the worker threads in line
 **
 ** @param tcp Thread data
 **   *li *c tcp->rep  ZMQ Response socket into which to throw our response
 **
 ** @param job
 */

void isRsyncConnectionTest2(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isRsyncConnectionTest2";
  const char *hostName;         // host name to look up
  const char *userName;
  //  const char *localDir;
  const char *destDir;
  char userAtHost[256];
  char *dfCmd;
  int dfCmdSize;
  isSubProcess_type sp;
  isSubProcessFD_type fds[2];
  int err;
  int zerr;
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // send back the object that we are operating on
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *rtn_str;                // string version of our object
  char *job_str;                // string version of job
  char *envp[] = {
    NULL                                // -- Required NULL at end of list
  };
  char *argv[] = {
    "ssh",                              // 0
    "-v",                               // 1
    "-o",                               // 2
    "StrictHostKeyChecking=no",         // 3
    "-o",                               // 4
    "PasswordAuthentication=no",        // 5
    "-o",                               // 6
    "KbdInteractiveDevices=none",       // 7
    "-o",                               // 8
    "ConnectTimeout=5",                 // 9
    NULL,                               // 10 userAtHost
    NULL,                               // 11 dfCmd
    NULL                                // -- Required NULL end of list
  };

  // Todo: add regexp tests
  //
  hostName = json_string_value(json_object_get(job, "remoteHostName"));
  userName = json_string_value(json_object_get(job, "remoteUserName"));
  destDir  = json_string_value(json_object_get(job, "remoteDirName"));

  snprintf(userAtHost, sizeof(userAtHost)-1, "%s@%s", userName, hostName);
  userAtHost[sizeof(userAtHost)-1] = 0;
  argv[10] = userAtHost;

  dfCmdSize = 2*strlen(destDir) + 65;
  dfCmd = calloc(dfCmdSize, 1);
  if (dfCmd == NULL) {
    isLogging_crit("%s: Out of memory (isRsync dfCmd)", id);
    exit(-1);
  }

  snprintf(dfCmd, dfCmdSize-1, "sh -c \"mkdir -p %s && df -h %s\"", destDir, destDir);
  dfCmd[dfCmdSize-1] = 0;
  argv[11] = dfCmd;


  isLogging_info("%s: request to check connection to %s", id, hostName);

  //
  // Really we just send a blank error message: actual errors are
  // signaled by is_zmq_error_reply
  //
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

  zerr = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
    pthread_exit(NULL);
  }

  rtn_json = json_object();

  void stdoutReader(char *s) {
    isLogging_info("%s: Receiving stdout\n%s", id, s);
    json_object_set_new(rtn_json, "stdout", json_string(s));
  }
  void stderrReader(char *s) {
    isLogging_info("%s: Receiving stderr\n%s", id, s);
    json_object_set_new(rtn_json, "stderr", json_string(s));
  }

  sp.cmd  = "/usr/bin/ssh";
  sp.envp = envp;
  sp.argv = argv;
  sp.nfds = 2;
  sp.fds  = fds;

  // Set up stdout
  // This will be the result of "sh -c mkdir destDir | df -h dstDir"
  fds[0].fd     = 1;                    // child process's stdout
  fds[0].is_out = 1;                    // flag indicating we'll be reading this
  fds[0].read_lines = 0;                // Just accumulate the entire ouput
  fds[0].progressReporter = NULL;       // No progress reports
  fds[0].done = stdoutReader;           // routine to receive stdout result

  // Set up stderr
  // This will be the result mainly of the -v switch on ssh
  fds[1].fd     = 2;                    // child process's stderr
  fds[1].is_out = 1;                    // flag indicating we'll be reading this
  fds[1].read_lines = 0;                // Just accumulate the entire ouput
  fds[1].progressReporter = NULL;       // No progress reports
  fds[1].done = stderrReader;           // routine to receive stdout result
  
  err = isSubProcess(id, &sp);
  if (err) {
    isLogging_err("%s: isSubProcess failed", id);
  }

  set_json_object_integer(id, rtn_json, "status", sp.rtn);

  // Return message
  rtn_str = NULL;
  if (rtn_json != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    json_decref(rtn_json);
    pthread_mutex_unlock(&wctx->metaMutex);
  } else {
    rtn_str = strdup("");
  }

  zerr = zmq_msg_init_data(&rtn_msg, rtn_str, strlen(rtn_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: zmq_msg_init failed (rtn_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (rtn_str)", id);
    pthread_exit (NULL);
  }
  
  do {
    // Error Message
    zerr = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send rsync test: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    zerr = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr < 0) {
      isLogging_err("%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Results
    zerr = zmq_msg_send(&rtn_msg, tcp->rep, 0);
    if (zerr < 0) {
      isLogging_err("%s: sending rtn_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while(0);
}

void isRsyncTransfer(isWorkerContext_t *wctx, isThreadContextType *tcp, json_t *job) {
  static const char *id = FILEID "isRsyncTransfer";
  const char *hostName;         // host name to look up
  const char *userName;
  const char *localDir;
  const char *destDir;
  const char *progressPublisher;
  const char *progressAddress;
  const char *progressPort;
  const char *tag;
  redisContext *remote_redis;
  char *src;
  int src_size;
  char *dfCmd;
  int dfCmdSize;
  isSubProcess_type sp;
  isSubProcessFD_type fds[2];
  int err;
  int zerr;
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // send back the object that we are operating on
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *rtn_str;                // string version of our object
  char *job_str;                // string version of job

  char *envp[] = {              // Environment (if any) for the process
    NULL;                       // -- Required NULL at end of list
  };


  char *argv[] = {              // Argument list
    "rsync",                            // 0
    "-v",                               // 1
    "-rt",                              // 2
    "--progress",                       // 3
    "--partial",                        // 4
    "--partial-dir=.rsync_partial",     // 5
    "-e",                               // 6
    "''ssh -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o KbdInteractiveDevices=none''", // 7
    NULL,                               // 8 src
    NULL,                               // 9 dst
    NULL                                // -- Required NULL end of list
  };


  // Todo: add regexp tests
  //
  hostName = json_string_value(json_object_get(job, "remoteHostName"));
  userName = json_string_value(json_object_get(job, "remoteUserName"));
  destDir  = json_string_value(json_object_get(job, "remoteDirName"));
  localDir = json_string_value(json_object_get(job, "localDir"));

  tag               = json_string_value(json_object_get(job,  "tag"));
  progressPublisher = json_string_value(json_object_get(job,  "progressPublisher"));
  progressAddress   = json_string_value(json_object_get(job,  "progressAddress"));
  progressPort      = json_integer_value(json_object_get(job, "progressPort"));

  // full source length         @                      : 
  src_size = strlen(userName) + 1 + strlen(hostName) + 1 + strlen(localDir) + 2;
  src = calloc(src_size, sizeof(*src));
  if (src == NULL) {
    isLogging_crit("%s: Out of memory (src)", id);
    exit (-1);
  }

  snprintf(src, src_size-1, "%s@%s:%s", userName, hostName, localDir);
  src[src_size-1] =0;
  argv[8] = src;

  argv[9] = destDir;
  //
  // Really we just send a blank error message: actual errors are
  // signaled by is_zmq_error_reply
  //
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

  zerr = zmq_msg_init_data(&job_msg, job_str, strlen(job_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: sending rsync test result failed: %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (job_str)", id);
    pthread_exit(NULL);
  }

  rtn_json = json_object();

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

  void progress(char *s) {
    if (remote_redis && progressPublisher) {
      reply = redisCommand(remote_redis, "PUBLISH %s {\"progress\":\"%s\",\"done\":false,\"tag\":\"%s\"}", progressPublisher, s, tag);
      if (reply == NULL) {
        isLogging_info("%s: redis progress publisher %s returned error %s when publishing \"%*s\"", id, progressPublisher, rrc->errstr, strlen(s), s);
      } else {
        freeReplyObject(reply);
      }
    }
  }

  void stderrReader(char *s) {
    isLogging_info("%s: Receiving stderr\n%s", id, s);
    json_object_set_new(rtn_json, "stderr", json_string(s));
  }

  sp.cmd  = "/usr/bin/rsync";
  sp.envp = envp;
  sp.argv = argv;
  sp.nfds = 2;
  sp.fds  = fds;
  sp.rtn  = 505911;     // Flag indicating the sub process failed to run

  // Set up stdout
  // This will be mainly the result of the --progress switch
  //
  fds[0].fd     = 1;                    // child process's stdout
  fds[0].is_out = 1;                    // flag indicating we'll be reading this
  fds[0].read_lines = 0;                // Just accumulate the entire ouput
  fds[0].progressReporter = progress;   // No progress reports
  fds[0].done = NULL;                   // routine to receive stdout result

  // Set up stderr
  // This will be the result mainly of the -v switch
  fds[1].fd     = 2;                    // child process's stderr
  fds[1].is_out = 1;                    // flag indicating we'll be reading this
  fds[1].read_lines = 0;                // Just accumulate the entire ouput
  fds[1].progressReporter = NULL;       // No progress reports
  fds[1].done = stderrReader;           // routine to receive stdout result
  
  err = isSubProcess(id, &sp);
  if (err) {
    isLogging_err("%s: isSubProcess failed", id);
  }

  set_json_object_integer(id, rtn_json, "status", sp.rtn);

  // Return message
  rtn_str = NULL;
  if (rtn_json != NULL) {
    pthread_mutex_lock(&wctx->metaMutex);
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    json_decref(rtn_json);
    pthread_mutex_unlock(&wctx->metaMutex);
  } else {
    rtn_str = strdup("");
  }

  zerr = zmq_msg_init_data(&rtn_msg, rtn_str, strlen(rtn_str), is_zmq_free_fn, NULL);
  if (zerr == -1) {
    isLogging_err("%s: zmq_msg_init failed (rtn_str): %s\n", id, zmq_strerror(errno));
    is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Could not initialize reply message (rtn_str)", id);
    pthread_exit (NULL);
  }
  
  do {
    // Error Message
    zerr = zmq_msg_send(&err_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr == -1) {
      isLogging_err("%s: Could not send rsync test: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Job 
    zerr = zmq_msg_send(&job_msg, tcp->rep, ZMQ_SNDMORE);
    if (zerr < 0) {
      isLogging_err("%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }

    // Results
    zerr = zmq_msg_send(&rtn_msg, tcp->rep, 0);
    if (zerr < 0) {
      isLogging_err("%s: sending rtn_str failed: %s\n", id, zmq_strerror(errno));
      break;
    }
  } while(0);
}

