/** @file isSubProcess.c
 ** @copyright 2020 by Northwestern University All Rights Reserved
 ** @author Keith Brister
 ** @brief Fork subprocess and send streams to caller specified routines
 **
 ** Calling external functions and dealing with the results is a
 ** recurring theme.  This is an attempt to corral this into one
 ** place.
 **
 */

/** All hail single include file
 */
#include "is.h"








/** isSubProcess
 **
 **     Fatal errors kill program
 **
 ** @param[in]  cid   id of calling process to add to logging messages
 **
 ** 
 */
void isSubProcess(const char *cid, isSubProcess_type *spt) {
  static const char *id = FILEID "isSubProcess";
  static redisAsyncContext *subac;  
  static struct pollfd subfd;
  static int c;                 // process id of child process
  
  int max_fd;                   // largest file descriptor
  int err;                      // return value
  int keep_on_truckin;          // flag to stay in the poll loop
  int redis_running;            // Stay in poll loop until lowered
  int pollstat;                 // result of poll command
  struct pollfd *polllist;      // list of fd's to send to poll
  int npoll;                    // number of active fd's in polllist
  int i;                        // loop index
  int status;                   // return value from waitpid
  isSubProcessFD_type *fd_servers;      // Lookup table of isSubProcessFD's
  isSubProcessFD_type *spfdp;   // pointo into the fd_server array
  char tmp[256];                // temporary buffer to read data into
  int bytes_read;               // number of bytes read in the read statement

  void connectCB(const redisAsyncContext *ac, int status) {
    static const char *id = FILEID "isSubProcess->connectCB";
    isLogging_debug( "%s: status=%d", id, status);
  }


  /** call back in case a redis server becomes disconnected
   *  TODO: reconnect
   */
  void disconnectCB(const redisAsyncContext *ac, int status) {
    static const char *id = FILEID "isSubProcess->disconnectCB";
    if( status != REDIS_OK) {
      isLogging_err( "%s: Disconnected with status %d: %s", id, status, ac->errstr);
    }
    isLogging_debug( "%s: redis disconnected status %d", id, status);
    redis_running = 0;
  }

  /** hook to mange read events
   */
  void addRead( void *data) {
    struct pollfd *pfd;
    pfd = (struct pollfd *)data;

    if( (pfd->events & POLLIN) == 0) {
      pfd->events |= POLLIN;
    }
  }

  /** hook to manage "don't need to read" events
   */
  void delRead( void *data) {
    struct pollfd *pfd;
    pfd = (struct pollfd *)data;

    if( (pfd->events & POLLIN) != 0) {
      pfd->events &= ~POLLIN;
    }
  }

  /** hook to manage write events
   */
  void addWrite( void *data) {
    struct pollfd *pfd;
    pfd = (struct pollfd *)data;

    if( (pfd->events & POLLOUT) == 0) {
      pfd->events |= POLLOUT;
    }
  }

  /** hook to manage "don't need to write anymore" events 
   */
  void delWrite( void *data) {
    struct pollfd *pfd;
    pfd = (struct pollfd *)data;

    if( (pfd->events & POLLOUT) != 0) {
      pfd->events &= ~POLLOUT;
    }
  }
  /** hook to clean up
   * TODO: figure out what we are supposed to do here and do it
   */
  void cleanup( void *data) {
    struct pollfd *pfd;

    pfd = (struct pollfd *)data;

    pfd->fd = -1;

    if( (pfd->events & (POLLOUT | POLLIN)) != 0) {
      pfd->events &= ~(POLLOUT | POLLIN);
    }
  }

  //
  // Currently hiredis does not call this callback.  Instead an
  // unsubscribe message is sent to subCB.
  //
  void unSubCB(redisAsyncContext *ac, void *reply, void *privdata) {
    static const char *id = FILEID "isSubProcess->unSubDB";

    isLogging_debug("%s: unsubscribed", id);
  }

  /** Use the publication to request the new value
   */
  void subCB(redisAsyncContext *ac, void *reply, void *privdata) {
    static const char *id = FILEID "isSubProcess->subCB";
    json_t *msg;
    json_t *jsig;
    json_error_t loads_err;
    int sig;

    redisReply *r;
    //lsredis_obj_t *p;
    char *k;

    isLogging_debug("%s: start of cb  keep_on_truckin=%d", id, keep_on_truckin);

    if (reply == NULL) {
      // 
      // we're called with a null reply on disconnect.  Nothing for us to do.
      //
      return;
    }

    r = (redisReply *)reply;

    isLogging_debug("%s: got reply type %d", id, r->type);

    // Ignore our subscribe reply
    //
    if( r->type == REDIS_REPLY_ARRAY && r->elements == 3 && r->element[0]->type == REDIS_REPLY_STRING && strcmp( r->element[0]->str, "subscribe")==0) {
      isLogging_debug("%s: Ignoring subscribe message: %s", id, r->element[0]->str);
      return;
    }

    // Log stuff we don't understand
    //
    if( r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING) {

      isLogging_debug( "%s: unexpected reply", id);
      return;
    }

    isLogging_debug("%s: got reply string", id, r->element[2]->str);

    //
    // Ignore obvious junk
    //
    k = r->element[2]->str;

    if( k == NULL || *k == 0) {
      return;
    }

    isLogging_debug("%s: message received: \"%s\"", id, k);

    msg = json_loads(r->element[2]->str, 0, &loads_err); 
    if (msg == NULL) {
      isLogging_err("%s: bad json message: %s from %s", id, loads_err.text, k);
      return;
    }

    if (c > 0) {
      jsig = json_object_get(msg, "sig");
      if (jsig) {
        sig = json_integer_value(jsig);
        if( sig > 0) {
          kill(c, sig);
        }
      }
    }
    json_decref(msg);
  }


  //
  // Set up pipes.  Automatically close the new fds in the child after
  // execve.  All this activity here should make things simpler in the
  // polling loop.
  //
  max_fd = 0;
  for (i=0; i < spt->nfds; i++) {
    //
    // make sure at least we have a valid pointer to an empty string
    //
    spt->fds[i]._buf = realloc(NULL,1);
    spt->fds[i]._buf[0] = 0;
    spt->fds[i]._buf_size = 0;

    pipe2(spt->fds[i]._pipe, O_CLOEXEC);               // auto close parent end of pipe in child
    if (spt->fds[i].is_out) {
      spt->fds[i]._piped_fd = spt->fds[i]._pipe[0];     // copy to ease polling loop logic
      spt->fds[i]._event = POLLIN;                      // the event we'll be polling for
    } else {
      spt->fds[i]._piped_fd = spt->fds[i]._pipe[1];     // copy to ease polling loop logic
      spt->fds[i]._event = POLLOUT;                     // the event we'll be polling for
    }
    if (spt->fds[i]._piped_fd > max_fd) {
      max_fd = spt->fds[i]._piped_fd;
    }
  }

  c = fork();
  if (c == -1) {
    // fork failed
    isLogging_err( "%s->%s: Fork failed", cid, id);
    if (spt->onLaunch) {
      spt->rtn = 1;
      spt->onLaunch(strerror(errno));
    }
    return;
  }

  if (c == 0) {
    //
    // In Child
    //
    int ii;
    int old_errno;

    isLogging_debug("%s->%s: cmd: '%s'", cid, id, spt->cmd);

    isLogging_debug("%s->%s: argv dump", cid, id);
    for (ii=0; spt->argv[ii] != NULL; ii++) {
      isLogging_debug("%s->%s: %d  %s", cid, id, ii, spt->argv[ii]);
    }

    isLogging_debug("%s->%s: env dump", cid, id);
    for (ii=0; spt->envp[ii] != NULL; ii++) {
      isLogging_debug("%s->%s: %d  %s", cid, id, ii, spt->envp[ii]);
    }

    for (i=0; i < spt->nfds; i++) {
      if (spt->fds[i].is_out) {
        dup2(spt->fds[i]._pipe[1], spt->fds[i].fd);       // duplicate the child's fd to read from
      } else {
        dup2(spt->fds[i]._pipe[0], spt->fds[i].fd);       // duplicate the child's fd to write to
      }
    }

    execve( spt->cmd, spt->argv, spt->envp);
    //
    // execve never returns except on error
    //
    old_errno = errno;
    isLogging_err("%s->%s: execve failed for %s: %s\n", cid, id, spt->cmd, strerror(errno));

    //
    // If we are listening to stderr then we can send a message to the parent
    //
    fprintf(stderr, "Execve failed for %s: %s\n", spt->cmd, strerror(old_errno));

    //
    // None of the reasons for getting here are recoverable
    //
    _exit (-1);
  }

  // In parent
  
  //
  // No way to tell here if execve was (or will be) successful.
  // Caller should have asked to listen to stderr so we can at least
  // learn why things failed (if the did in fact fail)
  //
  if (spt->onLaunch) {
    spt->rtn = 0;
    spt->onLaunch(NULL);
  }

  isLogging_debug("%s->%s: about to connect to redis", cid, id);

  redis_running = 1;
  subac = redisAsyncConnect("127.0.0.1", 6379);
  if( subac->err) {
    isLogging_err( "%s: redisAsyncConnect Error: %s", id, subac->errstr);
    exit(-1);
  }
  subfd.fd           = subac->c.fd;
  subfd.events       = 0;
  subac->ev.data     = &subfd;
  subac->ev.addRead  = addRead;
  subac->ev.delRead  = delRead;
  subac->ev.addWrite = addWrite;
  subac->ev.delWrite = delWrite;
  subac->ev.cleanup  = cleanup;
  
  redisAsyncSetConnectCallback(subac, connectCB);
  redisAsyncSetDisconnectCallback(subac, disconnectCB);
  // Set up redis subscriber

  redisAsyncCommand(subac, subCB, NULL, "SUBSCRIBE isSubInput");

  isLogging_debug("%s->%s: max_fd=%d", cid, id, max_fd);

  fd_servers = calloc(max_fd+1, sizeof(*fd_servers));
  if (fd_servers == NULL) {
    isLogging_crit("%s->%s: Out of memory (fd_servers)", cid, id);
    _exit(-1);
  }  

  polllist = calloc(spt->nfds+1, sizeof (*polllist));
  if (polllist == NULL) {
    isLogging_crit("%s->%s: Out of memory (pollist)", cid, id);
    _exit(-1);
  }
  
  polllist[0].fd     = subfd.fd;
  for (npoll=1; npoll <= spt->nfds; npoll++) {
    isLogging_debug("%s->%s: event=%d  piped_fd=%d  child_fd=%d", cid, id, spt->fds[npoll]._event, spt->fds[npoll]._piped_fd, spt->fds[npoll].fd);
    fd_servers[spt->fds[npoll-1]._piped_fd] = spt->fds[npoll-1];  // find fds structure from file descriptor
    polllist[npoll].fd     = spt->fds[npoll-1]._piped_fd;
    polllist[npoll].events = spt->fds[npoll-1]._event;
  }
  isLogging_debug("%s->%s: npoll=%d", cid, id, npoll);

  //
  // Verify that our child is not prematurely dead
  //

  keep_on_truckin = 1;

  isLogging_debug("%s->%s: starting poll loop", cid, id);

  while(keep_on_truckin || redis_running) {
    // set redis events
    polllist[0].events = subfd.events;

    pollstat = poll(polllist, npoll, 100);
    if (pollstat == -1) {
      isLogging_err("%s->%s: poll failed: %s", cid, id, strerror(errno));
      exit (-1);
    }
    
    //isLogging_debug("%s->%s: pollstat=%d", cid, id, pollstat);

    //
    // Don't check on child if it's alread done for
    //
    if (keep_on_truckin && (pollstat == 0)) {
      //
      // Check to see if our process is still running
      //
      err = waitpid(c, &status, WNOHANG);
      if (err == c) {
        //
        // Normal way we expect to exit
        //
        if (WIFEXITED(status)) {
          spt->rtn = WEXITSTATUS(status);
        }
        keep_on_truckin = 0;
        isLogging_debug("%s: child exited, disconnecting redis", id);
        //
        redisAsyncCommand(subac, unSubCB, NULL, "UNSUBSCRIBE");
        redisAsyncDisconnect(subac);    // Don't need to listen to redis after sub process has ended
      }
      if (err == -1) {
        //
        // Likely our command was not found.  Sounds like one of those
        // very rare programming errors.
        //
        isLogging_info("%s: waitpid 2 failed: %s\n", id, strerror(errno));
        keep_on_truckin = 0;
        redisAsyncCommand(subac, unSubCB, NULL, "UNSUBSCRIBE");
        redisAsyncDisconnect(subac);    // Don't need to listen to redis after sub process has ended
      }
      //isLogging_debug("%s->%s: waitpid status %0x  keep_on_truckin: %d", cid, id, status, keep_on_truckin);
      continue;
    }
    
    // polllist[0] is the redis subscriber
    
    if (pollstat && polllist[0].revents) {
      isLogging_debug("%s->%s: redis subscriber event(s) %d", cid, id, polllist[0].revents);
      pollstat--;
      if (polllist[0].revents & POLLIN) {
        redisAsyncHandleRead(subac);
      }
      if (polllist[0].revents & POLLOUT) {
        redisAsyncHandleWrite(subac);
      }
    }

    for (i=1; pollstat && i<npoll; i++) {
      if (polllist[i].revents) {
        //isLogging_debug("%s->%s: fd: %d  revent: %d", cid, id, polllist[i].fd, polllist[i].revents);
        pollstat--;
      }

      // check for errors
      if (polllist[i].revents & (POLLERR | POLLHUP)) {
        isLogging_info("%s->%s: Error or hangup on fd %d\n", cid, id, polllist[i].fd);

        close(polllist[i].fd);
        polllist[i].fd = (polllist[i].fd < 0 ? polllist[i].fd : -polllist[i].fd);  // ignore this fd in the future
      }

      // we are reading something
      // 
      if (polllist[i].revents & POLLIN) {
        //isLogging_debug("%s->%s: POLLIN on fd=%d", cid, id, polllist[i].fd);

        spfdp = &fd_servers[abs(polllist[i].fd)];
        do {
          bytes_read = read(polllist[i].fd, tmp, sizeof(tmp)-1);

          //isLogging_debug("%s->%s: fd=%d read %d bytes: '%.*s'", cid, id, polllist[i].fd, bytes_read, bytes_read, tmp);

          if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
              break;
            }
            isLogging_err("%s->%s: error reading fd=%d  (child fd=%d): %s", cid, id, spfdp->_piped_fd, spfdp->fd, strerror(errno));
            //
            // Probably this is a programming error with no recovery.
            break;
          }
          //
          // Nulls are good.
          tmp[sizeof(tmp)-1] = 0;       // Just good form

          if (bytes_read > 0) {
            tmp[bytes_read] = 0;          // Really all the nullification that's needed

            //
            // Make room for incomming bytes
            //
            // New size includes temporary null byte
            //
            errno = 0;
            spfdp->_buf = realloc(spfdp->_buf, spfdp->_buf_size + bytes_read + 1);
            if (spfdp->_buf == NULL) {
              isLogging_err("%s->%s: Out of memory (realloc %d bytes): %s", cid, id, spfdp->_buf_size+bytes_read+1, strerror(errno));
              exit(-1);
            }
            //
            // point to next location to write to
            //
            spfdp->_buf_end = spfdp->_buf + spfdp->_buf_size;
            //
            // and write there (include our stray null)
            //
            memcpy(spfdp->_buf_end, tmp, bytes_read + 1);
            //
            // finally fix up size of buffer
            // 
            // Note we ignore our artifically added null so that it
            // will get overwritten next time we get a string
            //
            spfdp->_buf_size += bytes_read;
          }
        } while(0);     // shouldn't we be able to do this while nbytes > 0 if we set non-blocking?

        if (bytes_read > 0 && spfdp->onProgress) {
          if (spfdp->read_lines == 0) {
            // Easy as π
            spfdp->onProgress(spfdp->_buf);
            spfdp->_buf_size = 0;
          } else {
            //
            // line mode: parse out each line and send them one at a
            // time.  Save the residue (non \r or \n endings) for next
            // time around or for the very end when we clean things
            // up.
            //
            // strtok_r is mostly what we want except we need to know
            // when the last string has come our way.
            //
            char *saveptr;
            char *next;
            char *lastc;
            char *lasts;
            int save_last_string;

            // If the last char is \r or \n then we save the last
            // string from strtok_r for next time.
            //
            save_last_string = 1;
            if (strlen(spfdp->_buf) > 0) {
              lastc = spfdp->_buf + strlen(spfdp->_buf) - 1;
              if (*lastc == '\n' || *lastc == '\r') {
                save_last_string = 0;
              }
            }
            saveptr = NULL;     // style, saveptr is ignored on first strtok_r call
            lasts   = NULL;
            next = strtok_r(spfdp->_buf, "\r\n", &saveptr);
            while (next != NULL) {
              if (lasts != NULL) {
                spfdp->onProgress(lasts);
                lasts = next;
              }
              next = strtok_r(NULL, "\r\n", &saveptr);
            }
            if (lasts) {
              if (save_last_string) {
                memcpy(spfdp->_buf, lasts, strlen(lasts)+1);
                spfdp->_buf_size = strlen(lasts);
              } else {
                spfdp->onProgress(lasts);
                spfdp->_buf_size = 0;
              }
            }
          }
        }
      }

      if (polllist[i].revents & POLLOUT) {
        // Note that we do not have a use case for this yet.  Once we
        // do then this section can get filled out.
      }
    }
  }

  isLogging_debug("%s->%s: End of polling", cid, id);

  //
  // Send our results
  //
  for (i=1; i<npoll; i++) {
    if (polllist[i].fd > 0) {
      close(polllist[i].fd);
      //
      // set fd negative to get poll to ignore this one
      //
      // Practically fd is never zero since it comes from dup2 and
      // fd=0 is taken (stdin) hence anything returned by dup2 likely
      // starts at 3.  We can always subtract max_fd+1 to ensure a
      // negative number here if for some reasone dup2 starts
      // returning 0 fd's.
      //
      polllist[i].fd = -polllist[i].fd;
    }
    spfdp = &fd_servers[abs(polllist[i].fd)];
    if (spfdp->onProgress) {
      spfdp->onProgress(spfdp->_buf);
    }
    if (spfdp->onDone) {
      spfdp->onDone(spfdp->_buf);
    }
    free(spfdp->_buf);
    spfdp->_buf_size = 0;
  }

  //
  // Return memory allocations

  free(polllist);
  polllist = NULL;

  free(fd_servers);
  fd_servers = NULL;

  return;
}
