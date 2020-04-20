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

//
// File descriptor to handle
//
// We currently do not have a use case for handling stdin so beyond
// making a provision for this via is_out it is unsupported
//
typedef struct isSubProcessFD_struct {
  int fd;                               // child's file descriptor we want to pipe
  int is_out;                           // 0 = we write to (like stdin), 1 = we read from (like stdout)
  void *(progressReporter(char *));     // if not null this function handles the progress updates
  int line_or_raw;                      // 1 = send full lines only to progressReporter, 0 = whatever we have
  void *(done(char *));                 // if not null this function handles the final result
  int _piped_fd;                        // (private) parent's version of fd
  int _event;                           // (private) our poll event (POLLIN or POLLOUT)
  int _pipe[2];                         // (private) pipe between parent and child
  char *_buf;                           // our reading buffer
  char *_buf_end;                       // end of _s
  int _buf_size;                        // current number of buffer bytes
  isSubProcessFD_type *_next;           // (private) manage active polling objects via linked list
} isSubProcessFD_type;

typedef struct isSubProcess_struct {
  char *cmd;                            // name of executable to run
  char **envp;                          // null terminated list of environment variables
  char **argv;                          // null terminated list of arguments
  isSubProcessFD_type *fds;             // list of fds we are asked to manage
  int nfds;                             // number of fds in list
} isSubProcess_type;


/** isSubProcess
 **
 ** returns
 **     0 on success
 **     1 on failure
 **
 **     Fatal errors kill program
 **
 ** @param[in]  cid   id of calling process to add to logging messages
 **
 ** 
 */
int isSubProcess(const char *cid, isSubProcessType *spt) {
  static const char *id = FILEID "isSubProcess";

  int max_fd;                   // largest file descriptor
  int err;                      // return value
  int c;                        // process id of child process
  int keep_on_truckin;          // flag to stay in the poll loop
  int pollstat;                 // result of poll command
  struct pollfd *polllist;      // list of fd's to send to poll
  int npoll;                    // number of active fd's in polllist
  int i;                        // loop index
  isSubProcessFD_type *spfdp, *first_spfdp;
  

  //
  // Set up pipes.  Automatically close the new fds in the child after
  // execve.  All this activity here should make things simpler in the
  // polling loop.
  //
  first_spfdp = spt->fds[spt->nfds - 1];
  max_fd = 0;
  for (i=0; i < spt->nfds; i++) {
    spt->fds[i]._buf = NULL;
    spt->fds[i]._buf_size = 0;

    spt->_next = (i == 0 ? NULL : spt->nfds[i-1]);      // set up linked list
    pipe2(spt->nfds[i]._pipe, O_CLOEXEC);               // auto close parent end of pipe in child
    if (spt->fds[i].is_out) {
      dup2(spt->fds[i]._pipe[1], spt->fds[i].fd);       // duplicate the child's fd to read from
      spt->fds[i]._piped_fd = spt->fds[i]._pipe[1];     // copy to ease polling loop logic
      spt->fds[i]._event = POLLIN;                      // the event we'll be polling for
    } else {
      dup2(spt->fds[i]._pipe[0], spt->fds[i].fd);       // duplicate the child's fd to write to
      spt->fds[i]._piped_fd = spt->fds[i]._pipe[0];     // copy to ease polling loop logic
      spt->fds[i]._event = POLLOUT;                     // the event we'll be polling for
    }
    if (spt->fds[i]._piped_fd > max_fd) {
      max_fd = spt->fds[i];
    }
  }

  c = fork();
  if (c == -1) {
    // fork failed
    isLogging_err( "%s->%s: Fork failed", cid, id);
    return (1);
  }

  if (c == 0) {
    // In child
    
    execve( spt->cmd, spt->argv, spt->envp);
    //
    // execve never returns except on error
    //

    isLogging_err("%s->%s: execve failed: %s\n", cid, id, strerror(errno));
    //
    // None of the reasons for getting here are recoverable
    //
    exit (-1);
  }

  // In parent
  
  fd_servers = calloc(max_fd+1, sizeof(*fd_servers));
  if (fd_servers == NULL) {
    isLogging_crit("%s->%s: Out of memory (fd_servers)", cid, id);
    _exit(-1);
  }  

  polllist = calloc(spt->nfds, sizeof (*polllist));
  if (pollist == NULL) {
    isLogging_crit("%s->%s: Out of memory (pollist)", cid, id);
    _exit(-1);
  }
  
  for (npoll=0; npoll < spt->nfds; npoll++) {
    fd_servers[spt->fds[i]._piped_fd] = spt->fds + npoll;  // find fds structure from file descriptor
  }

  keep_on_truckin = 1;
  
  for (npoll=0, spfdp=first_spfdp; spfdp=spfdp->next; spfdp != NULL) {
    if (spfdp->_piped_fd >= 0) {
      pollist[npoll].fd = spfd->_piped_fd;
      pollist[npoll].event = spfd->_event;
      npoll++;
    }
  }                       

  do {
    pollstat = poll(polllist, npoll, 100);
    if (pollstat == -1) {
      isLogging_err("%s->%s: poll failed: %s", cid, id, strerror(errno));
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
        isLogging_info("%s->%s: Error or hangup on fd %d\n", cid, id, polllist[i].fd);

        close(polllist[i].fd);
        pollist[i].fd = (pollist[i].fd < 0 ? pollist[i].fd : -pollist[i].fd);  // ignore this fd in the future
      }

      // we are reading something
      // 
      if (polllist[i].revents & POLLIN) {
        spfdp = fd_servers[polllist[i].fd];
        do {
          char tmp[256];
          int bytes_read;

          bytes_read = read(polllist[i].fd, tmp, sizeof(tmp));

          if (bytes_read > 0) {
            //
            // Make room for incomming bytes
            //
            spfdp->_buf = realloc(spfdp->_buf, spfdp->_buf_size + bytes_read);
            //
            // point to next location to write to
            //
            spfdp->_buf_end = spfdp->_buf + spfdp->_buf_size;
            //
            // and write there
            //
            memcpy(spfdp->_buf_end, tmp, bytes_read);
            //
            // finally fix up size of buffer
            //
            spfdp->_buf_size += bytes_read;
          }
        } while(0);     // shouldn't we be able to do this while nbytes > 0 if we set non-blocking?
      }

      if (polllist[i].revents & POLLOUT) {
        // Note that we do not have a use case for this yet.  Once we
        // do then this section can get filled out.
      }

    } while (keep_on_truckin);

    
  

}
