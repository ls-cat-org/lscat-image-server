/*! @file isRsync.c
 *  @copyright 2020 by Northwestern University
 *  @author Keith Brister
 *  @brief Support for Rsync
 */
#include "is.h"

//
// Tracks rsync recovery processes so that we periodically do a
// waitpid to prevent zombies
//
static int nRecoveryProcesses;
static int *recoveryProcesses;


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
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
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
  
  rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    
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
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
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
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    
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
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
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
  // == ======
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
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    json_decref(rtn_json);
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
  static const char *hostName;         // host name to look up
  static const char *userName;
  static const char *controlPublisher;
  static const char *controlAddress;
  int controlPort;
//  static const char *localDir;
  static const char *destDir;
  static char userAtHost[256];
  static char *dfCmd;
  static int dfCmdSize;
  static isSubProcess_type sp;          // static to ensure uninitialized fields are null
  static isSubProcessFD_type fds[2];    // static to ensure uninitialized fields are null
  int launch_failed;
  int zerr;
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // send back the object that we are operating on
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *rtn_str;                // string version of our object
  char *job_str;                // string version of job
  const char *envp[] = {
    NULL                        // -- Required NULL at end of list
  };
  const char *argv[] = {
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
  hostName          = json_string_value(json_object_get(job, "remoteHostName"));
  userName          = json_string_value(json_object_get(job, "remoteUserName"));
  destDir           = json_string_value(json_object_get(job, "remoteDirName"));

  controlPublisher  = json_string_value(json_object_get(job,  "controlPublisher"));
  controlAddress    = json_string_value(json_object_get(job,  "controlAddress"));
  controlPort       = json_integer_value(json_object_get(job, "controlPort"));

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
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
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

  void onDoneStdout(char *s) {
    isLogging_info("%s: Receiving stdout\n%s", id, s);
    json_object_set_new(rtn_json, "stdout", json_string(s));
  }
  void onDoneStderr(char *s) {
    isLogging_info("%s: Receiving stderr\n%s", id, s);
    json_object_set_new(rtn_json, "stderr", json_string(s));
  }

  sp.cmd  = "/usr/bin/ssh";
  sp.envp = (char **)envp;
  sp.argv = (char **)argv;
  sp.nfds = 2;
  sp.fds  = fds;
  sp.controlPublisher = controlPublisher;
  sp.controlAddress   = controlAddress;
  sp.controlPort      = controlPort;

  // Set up stdout
  // This will be the result of "sh -c mkdir destDir | df -h dstDir"
  fds[0].fd     = 1;                    // child process's stdout
  fds[0].is_out = 1;                    // flag indicating we'll be reading this
  fds[0].read_lines = 0;                // Just accumulate the entire ouput
  fds[0].onProgress = NULL;             // No progress reports
  fds[0].onDone = onDoneStdout;         // routine to receive stdout result

  // Set up stderr
  // This will be the result mainly of the -v switch on ssh
  fds[1].fd     = 2;                    // child process's stderr
  fds[1].is_out = 1;                    // flag indicating we'll be reading this
  fds[1].read_lines = 0;                // Just accumulate the entire ouput
  fds[1].onProgress = NULL;             // No progress reports
  fds[1].onDone = onDoneStderr;         // routine to receive stdout result
  
  launch_failed = 0;
  void onLaunch(char *msg, int child_pid) {
    isLogging_debug("%s: onLaunch with status %d, child_pid: %d", id, sp.rtn, child_pid);
    if (sp.rtn) {
      launch_failed = 1;
      isLogging_err("%s: launch failed %s: %s", id, sp.rtn==1 ? "at fork" : "at execve", msg==NULL ? "No message" : msg);
      is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Process failed to start: %s: %s", id, sp.rtn==1 ? "at fork" : "at execve", msg==NULL ? "No message" : msg);
      json_decref(rtn_json);
    }
  }
  sp.onLaunch = onLaunch;

  isSubProcess(id, &sp, &wctx->metaMutex);
  if (launch_failed) {
    //
    // If we get here an error response has already been sent from
    // onLaunch, Nothing more for us to do.
    //
    return;
  }
  
  set_json_object_integer(id, rtn_json, "status", sp.rtn);

  // Return message
  rtn_str = NULL;
  if (rtn_json != NULL) {
    rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
    json_decref(rtn_json);
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
  const char   *hostName;         // host name to look up
  const char   *userName;
  const char   *localDir;
  //int           esaf;
  const char   *destDir;
  const char   *progressPublisher;
  const char   *progressAddress;
  int           progressPort;
  const char   *controlPublisher;
  const char   *controlAddress;
  int           controlPort;
  const char   *tag;
  redisContext *remote_redis;
  regex_t rec;
  regex_t rec2;
  int progress2;
  redisReply *reply;                    // used to deal with local job storage
  char *src;
  int src_size;
  char *dst;
  int dst_size;
  isSubProcess_type sp;
  isSubProcessFD_type fds[2];
  int zerr;
  zmq_msg_t err_msg;            // error message to send via zmq
  zmq_msg_t job_msg;            // send back the object that we are operating on
  zmq_msg_t rtn_msg;            // the job message
  json_t *rtn_json;             // Object to return
  char *rtn_str;                // string version of our object
  char *job_str;                // string version of job

  const char *envp[] = {        // Environment (if any) for the process
    NULL                        // -- Required NULL at end of list
  };


  const char *argv[] = {              // Argument list
    "rsync",                            // 0
    "-v",                               // 1
    "-rt",                              // 2
    "--info=progress2",                 // 3
    "--info=name0",                     // 4
    "--partial",                        // 5
    "--partial-dir=.rsync_partial",     // 6
    "-e",                               // 7
    "''ssh -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o KbdInteractiveDevices=none''", // 8
    NULL,                               // 9 src
    NULL,                               // 10 dst
    NULL                                // -- Required NULL end of list
  };


  //
  // Compile regular expressions for using progress reports
  //
  regcomp(&rec,  "([0-9]+)%.*to-chk([0-9]+)/([0-9]+)", REG_EXTENDED);
  regcomp(&rec2, "([0-9]+)%", REG_EXTENDED);
  progress2 = -1;

  // Todo: add regexp tests
  //
  hostName = json_string_value(json_object_get(job, "remoteHostName"));
  userName = json_string_value(json_object_get(job, "remoteUserName"));
  destDir  = json_string_value(json_object_get(job, "remoteDirName"));
  localDir = json_string_value(json_object_get(job, "localDirName"));
  //esaf     = json_integer_value(json_object_get(job, "esaf"));

  tag               = json_string_value(json_object_get(job,  "tag"));
  progressPublisher = json_string_value(json_object_get(job,  "progressPublisher"));
  progressAddress   = json_string_value(json_object_get(job,  "progressAddress"));
  progressPort      = json_integer_value(json_object_get(job, "progressPort"));

  controlPublisher  = json_string_value(json_object_get(job,  "controlPublisher"));
  controlAddress    = json_string_value(json_object_get(job,  "controlAddress"));
  controlPort       = json_integer_value(json_object_get(job, "controlPort"));

  isLogging_debug("%s: hostName=%s  userName=%s destDir=%s localDir=%s  tag=%s progressPublisher=%s progressAddress=%s  progressPort=%d, controlPublisher=%s controlAddress=%s  controlPort=%d",
                  id, hostName, userName, destDir, localDir, tag, progressPublisher, progressAddress, progressPort, controlPublisher, controlAddress, controlPort);


  // src    ~e [esaf] /                      nulls
  //              
  src_size = 2 + 12 + 1 + strlen(localDir) + 2;
  src = calloc(src_size, sizeof(*src));
  if (src == NULL) {
    isLogging_crit("%s: Out of memory (src)", id);
    exit (-1);
  }

  snprintf(src, src_size-1, "%s", localDir);
  src[src_size-1] = 0;
  argv[9] = src;

  //                            @                      :
  dst_size = strlen(userName) + 1 + strlen(hostName) + 1 + strlen(destDir) + 2;
  dst = calloc(dst_size, sizeof(*dst));
  if (dst == NULL) {
    isLogging_crit("%s: Out of memory (dst)", id);
    exit(-1);
  }

  snprintf(dst, dst_size-1, "%s@%s:%s", userName, hostName, destDir);
  dst[dst_size-1] = 0;
  argv[10] = dst;

  isLogging_debug("%s: src='%s'  dst='%s'", id, src, dst);

  
  //
  // On restart recovery we don't have a zmq connection so we
  // shouldn't start committing resources here that wont be freed
  //
  if (tcp->rep) {
    //
    // Really we just send a blank error message: actual errors are
    // signaled by is_zmq_error_reply
    //
    zmq_msg_init(&err_msg);

    // Job message part
    job_str = NULL;
    if (job != NULL) {
      job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
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
  }


  //
  // We'll connect to a local redis so we can save the job info once
  // se know the process id that has been successfully started
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

  int onStdoutProgress(char *s) {
    redisReply *reply;
    json_t *jmsg;
    char *msg;
    int progress;
    regmatch_t pmatch[4];
    int err;
    float n, d;
    int files_total;
    int files_remaining;

    files_total      = -1;
    files_remaining = -1;

    progress  = -1;

    err = regexec(&rec, s, 4, pmatch, 0);
    if (!err) {
      //
      // we have the file counts
      //
      progress = strtol(s+pmatch[1].rm_so, NULL, 10);
      files_remaining = strtol(s+pmatch[2].rm_so, NULL, 10);
      files_total     = strtol(s+pmatch[3].rm_so, NULL, 10);
      n = strtol(s+pmatch[2].rm_so, NULL, 10);
      d = strtol(s+pmatch[3].rm_so, NULL, 10);
      if (d > 0.0) {
        progress2 = floor((1.0 - n/d) * 100. + 0.5);
        if (progress2 > 100) {
          progress2 = 100;
        }
        if (progress2 < 0) {
          progress2 = 0;
        }
        if (progress2 > progress) {
          progress = progress2;
        }
      }
    } else {
      //
      // We do not have the file counts
      //
      err = regexec(&rec2, s, 2, pmatch, 0);
      if (!err) {
        progress = strtol(s+pmatch[1].rm_so, NULL, 10);
        if (progress2 > progress) {
          progress = progress2;
        }
      }
    }

    if (remote_redis && progressPublisher) {
      jmsg = json_object();
      json_object_set_new(jmsg, "stdout", json_string(s));
      json_object_set_new(jmsg, "done", json_false());
      json_object_set_new(jmsg, "tag", json_string(tag));
      if (progress > 0) {
        json_object_set_new(jmsg, "progress", json_integer(progress));
      }
      if (files_remaining > 0) {
        json_object_set_new(jmsg, "files_remaining", json_integer(files_remaining));
      }

      if (files_total > 0) {
        json_object_set_new(jmsg, "files_total", json_integer(files_remaining));
      }

      msg = json_dumps(jmsg, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
      json_decref(jmsg);

      reply = redisCommand(remote_redis, "PUBLISH %s %s", progressPublisher, msg);
      if (reply == NULL) {
        isLogging_info("%s: redis progress publisher %s returned error %s when publishing \"%.*s\"", id, progressPublisher, remote_redis->errstr, strlen(s), s);
      } else {
        freeReplyObject(reply);
      }
      free(msg);
    }

    return progress;
  }

  void onStdoutDone(char *s) {
    redisReply *reply;
    json_t *jmsg;
    char *msg;

    if (remote_redis && progressPublisher) {
      jmsg = json_object();
      //      json_object_set_new(jmsg, "stdout", json_string(s));
      json_object_set_new(jmsg, "done", json_true());
      json_object_set_new(jmsg, "tag", json_string(tag));
      msg = json_dumps(jmsg, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
      json_decref(jmsg);

      reply = redisCommand(remote_redis, "PUBLISH %s %s", progressPublisher, msg);
      if (reply == NULL) {
        isLogging_info("%s: redis progress publisher %s returned error %s when done", id, progressPublisher, remote_redis->errstr);
      } else {
        freeReplyObject(reply);
      }
      free(msg);
    }
  }

  int onStderrProgress(char *s) {
    redisReply *reply;
    json_t *jmsg;
    char *msg;

    if (remote_redis && progressPublisher) {
      jmsg = json_object();
      json_object_set_new(jmsg, "stderr", json_string(s));
      json_object_set_new(jmsg, "done", json_true());
      json_object_set_new(jmsg, "tag", json_string(tag));
      msg = json_dumps(jmsg, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
      json_decref(jmsg);
      reply = redisCommand(remote_redis, "PUBLISH %s %s", progressPublisher, msg);
      if (reply == NULL) {
        isLogging_info("%s: redis stderr publisher %s returned error %s when publishing \"%.*s\"", id, progressPublisher, remote_redis->errstr, strlen(s), s);
      } else {
        freeReplyObject(reply);
      }
      free(msg);
    }
    return -1;
  }

   void onStderrDone(char *s) {
    redisReply *reply;
    json_t *jmsg;
    char *msg;

    if (remote_redis && progressPublisher) {
      jmsg = json_object();
      //json_object_set_new(jmsg, "stderr", json_string(s));
      json_object_set_new(jmsg, "done", json_true());
      json_object_set_new(jmsg, "tag", json_string(tag));
      msg = json_dumps(jmsg, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
      json_decref(jmsg);
      reply = redisCommand(remote_redis, "PUBLISH %s %s", progressPublisher, msg);
      if (reply == NULL) {
        isLogging_info("%s: redis stderr publisher %s returned error %s when publishing done", id, progressPublisher, remote_redis->errstr);
      } else {
        freeReplyObject(reply);
      }
      free(msg);
    }
  }

  sp.cmd              = "/usr/bin/rsync";
  sp.envp             = (char **)envp;
  sp.argv             = (char **)argv;
  sp.nfds             = 2;
  sp.fds              = fds;
  sp.controlAddress   = controlAddress;
  sp.controlPort      = controlPort;
  sp.controlPublisher = controlPublisher;

  sp.rtn  = 505911;     // Flag indicating the sub process failed to run

  // Set up stdout
  // This will be mainly the result of the --progress switch
  //
  fds[0].fd         = 1;                // child process's stdout
  fds[0].is_out     = 1;                // (read from pipe[0])
  fds[0].read_lines = 1;                // Send as is
  fds[0].onProgress = onStdoutProgress; // Progress reports
  fds[0].onDone     = onStdoutDone;     // routine to receive stdout result

  // Set up stderr
  // This will be the result mainly of the -v switch
  fds[1].fd         = 2;                // child process's stderr
  fds[1].is_out     = 1;                // flag for reading (listen to pipe[0])
  fds[1].read_lines = 1;                // Send it as you get it
  fds[1].onProgress = onStderrProgress; // "Realtime" debug output
  fds[1].onDone     = onStderrDone;     // routine to receive stdout result
  

  //
  // Since we expect rsync to run a while we'll return from our zmq
  // call with the status of the launched sub process
  //
  void onLaunch(char *msg, int child_pid) {
    char *job_str2;             // note that the previous job_str is own by zmq and will be freed wnenever zmq is ready

    job_str2 = NULL;
    if (job != NULL) {
      json_object_set_new(job, "uid",      json_integer(geteuid()));
      json_object_set_new(job, "gid",      json_integer(getegid()));
      json_object_set_new(job, "childPid", json_integer(child_pid));

      job_str2 = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);

      //
      // Set an indication that we are running a rsync job so we can restart it when the image server is restarted
      //
      //  TODO: report an error if this command failes
      reply = redisCommand(tcp->rc, "HSET RSYNCS %s %s", tag, job_str2);
      if (reply) {
        freeReplyObject(reply);
      }
      free(job_str2);
    } 

    //
    // The rest is all zmq messaging.  If we are recovering from a restart then there is no zmq messaging to perform so just leave now.
    //
    if (tcp->rep == NULL) {
      return;
    }

    //
    // sp.rtn = 0 on success
    //        = 1 on fork failure
    // msg    = reason
    //
    if (sp.rtn) {
      isLogging_err("%s: Subproccess Failed %s: %s", id, sp.rtn==1 ? "at fork" : "at execve", msg==NULL ? "unknown reason" : msg);
      is_zmq_error_reply(NULL, 0, tcp->rep, "%s: Process failed to start: %s: %s", id, sp.rtn==1 ? "at fork" : "at execve", msg==NULL ? "No message" : msg);
      json_decref(rtn_json);
      return;
    }

    set_json_object_integer(id, rtn_json, "status", sp.rtn);  // sp.rtn is always 0 here

    // Return message
    rtn_str = NULL;
    if (rtn_json != NULL) {
      rtn_str = json_dumps(rtn_json, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
      json_decref(rtn_json);
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


  sp.onLaunch = onLaunch;

  isSubProcess(id, &sp, &wctx->metaMutex);

  reply = redisCommand(tcp->rc, "HDEL RSYNCS %s", tag);
  if (reply) {
    freeReplyObject(reply);
  }

  if (src) {
    free(src);
    src = NULL;
  }

  if (dst) {
    free(dst);
    dst = NULL;
  }
}

/**
 ** isRsyncRecover
 **
 ** Look for rsync jobs that may have been running when the image
 ** server last ran.  We'll kill any process ids that are still
 ** running and then restart the jobs.
 **
 ** If we just let jobs continue they'll still transfer data (good)
 ** but wont be updating status (bad).  Of course starting new
 ** versions of the jobs without first killing the old ones means
 ** we'll have two rsync processes going after the same files.  This
 ** is bad always.
 */
void isRsyncRecover() {
  static const char *id = FILEID "isRsyncRecover";
  redisContext *local_redis;
  redisReply *reply;
  redisReply *reply2;
  redisReply *subreply;
  int i;
  json_t *job;
  json_error_t jerr;
  int new_child;
  int pid;
  int expected_uid;
  int expected_gid;
  int err;
  char p[256];
  struct stat sbuf;
  char *key;
  
  local_redis = redisConnect("127.0.0.1", 6379);
  if (local_redis == NULL || local_redis->err) {
    if (local_redis) {
      isLogging_info("%s: Failed to connect to local redis %s:%d: %s", id, "127.0.0.1", 6879, local_redis->errstr);
    } else {
      isLogging_info("%s: Failed to connect to local redis %s:%d", id, "127.0.0.1", 6879);
    }
    return;
  }

  //
  // The current rsync jobs are in the local redis hash "RSYNCS" with
  // the unique identifer (aka: tag) as the hash key
  //
  reply = redisCommand( local_redis, "HGETALL RSYNCS");
  if (reply == NULL) {
    isLogging_err("%s: Redis error (HGETALL RSYNCS): %s\n", id, local_redis->errstr);
    exit(-1);
  }

  if (reply->type == REDIS_REPLY_ERROR) {
    isLogging_err("%s: Reids HGETALL RSYNCS produced an error: %s\n", id, reply->str);
    exit(-1);
  }

  if (reply->type == REDIS_REPLY_NIL) {
    // Nothing to do
    freeReplyObject(reply);
    redisFree(local_redis);
    return;
  }

  if (reply->type != REDIS_REPLY_ARRAY) {
    isLogging_err("%s: unexpected reply type %d", id, reply->type);
    freeReplyObject(reply);
    redisFree(local_redis);
    return;
  }

  nRecoveryProcesses = reply->elements / 2;
  recoveryProcesses = calloc(nRecoveryProcesses, sizeof(nRecoveryProcesses));
  if (recoveryProcesses == NULL) {
    isLogging_crit("%s: out of membery (recoveryProcesses)");
    _exit(-1);
  }

  for(i=0; i<reply->elements; i++) {
    subreply = reply->element[i];
    if (subreply->type != REDIS_REPLY_STRING) {
      isLogging_err("%s: unexpectid reply type in subreply %d", id, subreply->type);
      continue;
    }

    if ((i+1) % 2) {
      // Even elements are the keys
      key = subreply->str;
      continue;
    }

    // we are not multi threaded here
    job = json_loads(subreply->str, 0, &jerr);
    if (job == NULL) {
      isLogging_err("%s: Failed to parse RSYNCS key: %s", id, jerr.text);
      isLogging_err("%s: Attempting to parse: %s", id, subreply->str);
      continue;
    }

    pid = json_integer_value(json_object_get(job, "childPid"));
    if (pid <= 0) {
      isLogging_err("%s: childPid %d for RSYNCS hash %s", id, pid, subreply->str);
      json_decref(job);
      continue;
    }

    expected_uid = json_integer_value(json_object_get(job, "uid"));
    if (expected_uid <= 0) {
      isLogging_err("%s: bad uid %s for RSYNCS %s: %s", id, expected_uid, key, subreply->str);
      json_decref(job);
      continue;
    }

    expected_gid = json_integer_value(json_object_get(job, "gid"));
    if (expected_gid <= 0) {
      isLogging_err("%s: bad gid %d for RSYNCS %s: %s", expected_gid, id, key, subreply->str);
      json_decref(job);
      continue;
    }

    snprintf(p, sizeof(p)-1, "/proc/%d", pid);
    p[sizeof(p)-1] = 0;

    //
    // We are going to kill the process if it exists and it has the
    // ownership we are expecting.  There are lots of reasons that
    // stat might fail but we are not going to fine tune our response
    // at this point: err=0 we'll try to kill the proccess, anything
    // else we wont.
    //
    err = stat(p, &sbuf);
    if (!err) {
      if (sbuf.st_uid == expected_uid && sbuf.st_gid == expected_gid) {
        //
        //  Perhaps we should care about the return statuses of each
        //  of these calls.
        //
        kill(pid, 2);
        sleep(1);
        kill(pid, 9);
      }
    }

    //
    // Remove this key from the redis hash.
    //
    reply2 = redisCommand(local_redis, "HDEL RSYNCS %s", key);
    freeReplyObject(reply2);


    //
    // Here we can resonably expect the old process is gone and we can safely set up a new one.
    //
    new_child = fork();
    if (new_child == -1) {
      isLogging_crit("%s: fork failed: %s", id, strerror(errno));
      exit(-1);
    }

    if (new_child > 0) {
      //
      // In parernt
      //
      recoveryProcesses[i/2] = new_child;
    }

    if (new_child == 0) {
      //
      // In child
      //
      isWorkerContext_t wc;
      pthread_mutexattr_t matt;
      isThreadContextType tc;
      struct passwd *esaf_pwds;
      const char *homeDirectory;            // ESAF user's home directory

      //
      // This should be the esaf account info
      //
      errno = 0;
      esaf_pwds = getpwuid(expected_gid);
      if (esaf_pwds == NULL) {
        isLogging_err("%s: getpwuid failed: %s", id, strerror(errno));
        _exit(1);
      }

      homeDirectory = esaf_pwds->pw_dir;
      if(chdir(homeDirectory)) {
        isLogging_err("%s: could not change to directory %s: %s",
                       id, homeDirectory, strerror(errno));
        _exit(1);
      }

      setgid(expected_gid);
      setuid(expected_uid);

      //
      // Worker context pointers and ints.
      //
      // We do not use much for rsync support and we are not using zmq
      // as our status will be reported via redis.
      //
      wc.key          = NULL;
      wc.n_buffers   = 0;
      wc.max_buffers = 0;
      wc.zctx        = NULL;
      wc.router      = NULL;
      wc.dealer      = NULL;

      //
      // ctxMutex is not currently used by isRsync.  However,we
      // initialize it since we'd otherwise have nonsence.
      //
      pthread_mutexattr_init(&matt);
      pthread_mutexattr_settype(&matt, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutexattr_setpshared(&matt, PTHREAD_PROCESS_SHARED);
      pthread_mutex_init(&wc.ctxMutex, &matt);
      pthread_mutexattr_destroy(&matt);

      //
      // We do use metaMutex, however, we will not actual be multi
      // threaded.  It's easier to define it than to have a
      // non-threading flag that we have to carry around.
      //
      pthread_mutex_init(&wc.metaMutex, NULL);

      
      // no zmq communication for us
      tc.rep = NULL;
      //
      // setup redis
      //
      tc.rc = redisConnect("127.0.0.1", 6379);
      if (tc.rc == NULL || tc.rc->err) {
        if (tc.rc != NULL) {
          isLogging_err("%s: Failed to connect to redis: %s\n", id, tc.rc->errstr);
        } else {
          isLogging_err("%s: Failed to get redis context\n", id);
        }
        fflush(stderr);
        _exit (-1);
      }

      isRsyncTransfer(&wc, &tc, job);

      json_decref(job);
      pthread_mutex_destroy(&wc.metaMutex);
      redisFree(tc.rc);

      _exit(0);
      //
      // End of child
      //
    }
  }
}

void isRsyncWaitpid() {
  static const char *id = FILEID "isRsyncWaitpid";
  int i;
  int status;
  int err;

  for(i=0; i<nRecoveryProcesses; i++) {
    if(recoveryProcesses[i] <= 0) {
      continue;
    }
    err = waitpid(recoveryProcesses[i], &status, WNOHANG);
    if (err == recoveryProcesses[i]) {
      //
      //
      if(WIFEXITED(status)) {
        isLogging_info("%s: process %d exited normally with status %d",
                       id, recoveryProcesses[i], WEXITSTATUS(status));
      } else if(WIFSIGNALED(status)) {
        isLogging_info("%s: process %d exited  with due to signal %d",
                       id, recoveryProcesses[i], WTERMSIG(status));
      } else {
        isLogging_info("%s: process %d terminated", id, recoveryProcesses[i]);
      }
      recoveryProcesses[i] = 0;
    }
  }
}


