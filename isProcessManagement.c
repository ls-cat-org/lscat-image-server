/** @file isProcessManagement.c
 ** @copyright 2017 by Northwestern University All Rights Reserved
 ** @author Keith Brister
 ** @brief Process Management Code for the LS-CAT Image Server Version 2
 **
 ** Processes are kept track of using a linked list of @c
 ** isProcessListType. In order to speed up access to a given process
 ** we use a hash table which is rebuilt whenever necessary from the
 ** linked list.
 **
 */

/** All header files are placed in @c is.h to ensure consistent definitions.
 */ 
#include "is.h"

/** Number of process table in to make room for initially       */
#define INITIAL_PROCESS_TABLE_SIZE 128

/** Current size of our proccess table                          */
static int process_table_size;

/** hash table for our processes                                */
static struct hsearch_data process_table;

/** First process in our linked list                            */
static isProcessListType *firstProcessListItem = NULL;
static zmq_pollitem_t *isZMQPollItems = NULL;
static int n_processes;

/** On startup ensure that other verions of this program are killed.
 */
void isInit(int dev_mode) {
  static const char *id = FILEID "isInit";
  FILE *our_pid_file;           // Read old pid and then write new pid
  pid_t old_pid;                // old pid: kill on sight
  int nconv;                    // 1 if successfully read pid from file, <1 otherwise
  int err;                      // kill function return value
  herr_t herr;                  // trap errors setting h5 error redirection

  isLogging_init(dev_mode);

  herr = H5Eset_auto2(H5E_DEFAULT, (H5E_auto2_t) is_h5_error_handler, stderr);
  if (herr < 0) {
    isLogging_crit("%s: Could not set HDF5 error reporting\n", id);
  }

  // Error recovery block
  //
  do {
    our_pid_file = fopen(dev_mode ? PID_DEV_FILE_NAME : PID_FILE_NAME, "r");
    if (our_pid_file == NULL) {
      // Assume we bombed out because the file does not yet exist
      // rather than some other more insidious reason.
      break;
    }
    
    errno = 0;
    nconv = fscanf(our_pid_file, "%d", &old_pid);
    if (nconv <= 0) {
      // Make sure we have read the file before we start killing
      // processes as root.
      //
      if (errno != 0) {
        isLogging_err("%s: failed to read pid file: %s\n", id, strerror(errno));
      }
      break;
    }

    //
    // Go for the throat right off the bat. TODO: consider being
    // gentle at first to give the previous process a chance to clean
    // up.
    err = kill(-old_pid, 9);
    if (err == -1 && errno != ESRCH) {
      isLogging_err("%s: Could not kill previous process: %s\n", id, strerror(errno));
      break;
    }
  } while (0);

  if (our_pid_file != NULL) {
    fclose(our_pid_file);
  }

  our_pid_file = fopen(dev_mode ? PID_DEV_FILE_NAME : PID_FILE_NAME, "w");
  if (our_pid_file == NULL) {
    isLogging_err("%s: Could not open pid file: %s\n", id, strerror(errno));
    exit (-1);
  }

  fprintf(our_pid_file, "%d", (int)getpid());
  fclose(our_pid_file);
}

/** Initialize our process list and hash table
 */
void isProcessListInit() {
  static const char *id = FILEID "isProcessListInit";
  int err;                                              // hcreate return value

  firstProcessListItem = NULL;                          // No entries yet
  process_table_size = INITIAL_PROCESS_TABLE_SIZE;      // Starter value

  err = hcreate_r(process_table_size, &process_table);
  if (err == 0) {
    isLogging_err("%s: hcreate_r fatal error: %s\n", id, strerror(errno));
    exit (-1);
  }
  n_processes = 0;
}


/** Launch a new process.
 **
 ** The process will run as the requesting user with the group number
 ** based on the esaf number in the home directory of the esaf user.
 ** Although we try to ensure that the user has access to the
 ** requested ESAF the operating system will back this up by refusing
 ** to allow access if the user is not, in fact, a member of the esaf
 ** group.
 **
 ** @param p  Object to launch
 **  @li @c p->isAuth      Our permissions object
 **  @li @c p->isAuth->uid Our user name
 **  @li @c p->esaf        The current ESAF
 **  @li @c p->processID   ID of forked child (so we can kill it later)
 */
void isStartProcess(isProcessListType *p) {
  static const char *id = FILEID "isStartProcess";
  int child;                            // Our child's process id returned by fork
  struct passwd *pwds;                  // Calling user's passwd entry
  struct passwd *esaf_pwds;             // ESAF user's passwd entry
  int uid;                              // Calling user's uid
  int gid;                              // ESAF user's uid (which will be our gid)
  const char *userName;                 // Our user's name
  const char *homeDirectory;            // ESAF user's home directory
  int err;                              // misc error return code
  char esafUser[16];                    // Generated ESAF user name

  userName = json_string_value(json_object_get(p->isAuth, "uid"));

  errno = 0;
  pwds = getpwnam(userName);
  if (pwds == NULL) {
    isLogging_err("%s: getpwnam failed for user %s: %s\n", id, userName, strerror(errno));
    return;
  }

  uid = pwds->pw_uid;

  if (p->esaf > 40000) {
    // We want to run with the UID of the calling user and the GID of the referenced ESAF
    //
    snprintf(esafUser, sizeof(esafUser)-1, "e%d", p->esaf);
    esafUser[sizeof(esafUser)-1] = 0;

    errno = 0;
    esaf_pwds = getpwnam(esafUser);
    if (esaf_pwds == NULL) {
      isLogging_err("%s: bad esaf user name '%s':%s\n", id, esafUser, strerror(errno));
      return;
    }
    gid = esaf_pwds->pw_gid;
    homeDirectory = strdup(esaf_pwds->pw_dir);
  } else {
    gid = pwds->pw_gid;
    homeDirectory = strdup(pwds->pw_dir);
  }

  isLogging_info("%s: Starting sub process: uid=%d, gid=%d, dir: %s,  User: %s  ESAF: %d\n", id, uid, gid, homeDirectory, userName, p->esaf);

  errno = 0;
  child = fork();
  if (child == -1) {
    isLogging_err("%s: Could not start sub process: %s\n", id, strerror(errno));
    exit (-1);
  }
  if (child) {
    //
    // In parent.
    //
    n_processes++;
    p->processID = child;
    return;
  }
  //
  // In child
  //

  errno = 0;
  err = setgid(gid);
  if (err != 0) {
    isLogging_err("%s: Could not change gid to %d: %s\n", id, gid, strerror(errno));
    _exit (-1);
  }

  errno = 0;
  err = setuid(uid);
  if (err != 0) {
    isLogging_err("Could not change uid to %d: %s\n", uid, strerror(errno));
    _exit (-1);
  }

  errno = 0;
  err = chdir(homeDirectory);
  if (err != 0) {
    isLogging_err("%s: Could not change working directory to %s: %s\n", id, homeDirectory, strerror(errno));
    _exit (-1);
  }

  isSupervisor(p->key);
}


/** Generate a new entry in the process linked list as well as the
 ** process hash table.
 **
 ** @param zctx The ZMQ context we'll use to set up communications
 **             with the process
 **
 ** @param isAuth Our permissions
 **   @li @c pid   Process identifier for this user instance
 **
 ** @param esaf   ESAF number our user is trying to access
 **
 ** @returns New process list item with ZMQ set up to communicate with the 
 */
isProcessListType *isCreateProcessListItem(void *zctx, json_t *isAuth, int esaf, int dev_mode) {
  static const char *id = FILEID "isCreateProcessListItem";
  char ourKey[128];
  char *pid;
  isProcessListType *rtn;
  char dealer_endpoint[256];
  int err;
  int socket_option;

  pid = (char *)json_string_value(json_object_get(isAuth, "pid"));

  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d%s", pid, esaf, dev_mode ? "-dev" : "");
  ourKey[sizeof(ourKey)-1] = 0;

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->key = strdup(ourKey);
  if (rtn->key == NULL) {
    isLogging_crit("%s: out of memory (rtn->key)\n", id);
    exit (-1);
  }
  rtn->processID = 0;
  rtn->esaf = esaf;
  rtn->isAuth = isAuth;
  json_incref(rtn->isAuth);
    
  rtn->parent_dealer = zmq_socket(zctx, ZMQ_DEALER);
  if (rtn->parent_dealer == NULL) {
    isLogging_err("%s: Could not create parent_dealer socket for %s: %s\n", id, rtn->key, zmq_strerror(errno));
    exit (-1);
  }
  
  socket_option = 0;
  err = zmq_setsockopt(rtn->parent_dealer, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set RCVWM for parent_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->parent_dealer, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    isLogging_err("%s: Could not set SNDWM for parent_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "ipc://@%s", rtn->key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;

  err = zmq_bind(rtn->parent_dealer, dealer_endpoint);
  if (err == -1) {
    isLogging_err("%s: Could not bind to socket %s: %s\n", id, dealer_endpoint, zmq_strerror(errno));
    exit (-1);
  }

  rtn->next = firstProcessListItem;
  firstProcessListItem = rtn;

  isRemakeZMQPollItems(NULL, NULL, NULL);

  isStartProcess(rtn);
  return rtn;
}


/** Recreate (or initially create) the zmq pollitems we need in the main
 ** loop to service the zmq sockets on which we rely
 **
 ** @param parent_router ZMQ socket to handle communication with
 **                      is_proxy.  Set to NULL to leave parent_router
 **                      unchanged.
 **
 ** @param err_rep       ZMQ socket to handle the error response.  Set to
 **                      NULL to leave untouched.
 **
 ** @param err_dealer    ZMQ socket to receive error messages from the
 **                      supervisors.  Set to NULL to leave untouched.
 **
 ** @returns List of pollites suitable for zmq_poll.
 **
 ** @todo We should really return a structure that also includes the
 ** number of items in the poll list.  We currently do this in an
 ** adhoc way that was somewhat error prone.
 */
zmq_pollitem_t *isRemakeZMQPollItems(void *parent_router, void *err_rep, void *err_dealer) {
  static const char *id = FILEID "isRemakeZMQPollItems";
  isProcessListType *plp;
  void *saved_parent_router;
  void *saved_err_dealer;
  void *saved_err_rep;
  int i;
  
  // Count the processes that appear to be running as indicated by a
  // non-null parent_dealer element.  Upon closing a process the
  // ProcessID will be non-zero while the parent_dealer should be
  // null, at least until we've successfully "waited" for the child
  // process to exit.
  //
  for (n_processes=0, plp=firstProcessListItem; plp != NULL; plp=plp->next) {
    if (plp->parent_dealer != NULL) {
      n_processes++;
    }
  }

  saved_parent_router = NULL;
  saved_err_rep       = NULL;
  saved_err_dealer    = NULL;
  if (isZMQPollItems != NULL) {
    saved_parent_router = isZMQPollItems[0].socket;
    saved_err_rep       = isZMQPollItems[1].socket;
    saved_err_dealer    = isZMQPollItems[2].socket;
    free (isZMQPollItems);
  }

  isZMQPollItems = calloc(3+n_processes, sizeof(*isZMQPollItems));
  if (isZMQPollItems == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  //
  isZMQPollItems[0].socket = parent_router ? parent_router : saved_parent_router;
  isZMQPollItems[0].events = ZMQ_POLLIN;

  isZMQPollItems[1].socket = err_rep ? err_rep : saved_err_rep;
  isZMQPollItems[1].events = ZMQ_POLLIN;

  isZMQPollItems[2].socket = err_dealer ? err_dealer : saved_err_dealer;
  isZMQPollItems[2].events = ZMQ_POLLIN;

  for (i=3, plp=firstProcessListItem; plp != NULL; plp = plp->next) {
    if (plp->parent_dealer != NULL) {
      isZMQPollItems[i].socket = plp->parent_dealer;
      isZMQPollItems[i].events = ZMQ_POLLIN;
      i++;
    }
  }
  return isZMQPollItems;
}

/** get the zmq poll items list
 **
 ** @returns Current list of poll items.
 */
zmq_pollitem_t *isGetZMQPollItems() {
  return isZMQPollItems;
}

/** number of processes
 **
 ** @returns Number of running processes
 */
int isNProcesses() {
  return n_processes;
}

/** Remove a process list item and recover its resources.
 **
 ** @param p Process list item to remove
 **
 */
void isDestroyProcessListItem(isProcessListType *p) {
  static const char *id = "isDestroyProcessListItem";
  isProcessListType *plp, *last;
  int err;

  last = NULL;

  // Remove process from list
  for(plp=firstProcessListItem; plp != NULL; plp = plp->next) {
    if (plp == p) {
      break;
    }
    last = plp;
  }

  if (plp != p) {
    isLogging_err("%s: could not find process %s\n", id, p->key);
    return;
  }

  err = zmq_close(p->parent_dealer);
  if (err == -1) {
    isLogging_err("%s: Could not close parent_dealer for %s: %s\n", id, p->key, zmq_strerror(errno));
  }

  p->parent_dealer = NULL;
  last->next = p->next;
  json_decref(p->isAuth);
  free((char *)p->key);
  free(p);

  isRemakeZMQPollItems(NULL, NULL, NULL);
}

/**  Recreate the hash table from the process linked list.  We check
 **  to be sure the process still exists and prune the list if need
 **  be.  This is called when we can no longer add anything more to
 **  the list.
 **
 **  @param rc Context of the redis instance we'll use to verify the
 **  existance of a process.
 **
 */
void remakeProcessList(redisContext *rc) {
  static const char *id = FILEID "remakeProcessList";
  ENTRY item;
  ENTRY *rtn_value;
  int err;
  isProcessListType *plp;
  int n_entries;
  const char *pid;
  redisReply *reply;
  int process_still_exists;
  
  //isLogging_info("%s: starting remake\n", id);

  hdestroy_r(&process_table);
  for (n_entries = 0, plp = firstProcessListItem; plp != NULL; plp = plp->next) {
    pid = json_string_value(json_object_get(plp->isAuth, "pid"));
    if (pid == NULL) {
      isLogging_err("%s: Failed to find pid in process list\n", id);
      exit (-1);
    }
    // See if the pid still exists
    //
    reply = redisCommand(rc, "EXISTS %s", pid);
    if (reply == NULL) {
      isLogging_err("%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
      isLogging_err("%s: redis exists returned unexpected type %d\n", id, reply->type);
      exit (-1);
    }
    process_still_exists = reply->integer;
    freeReplyObject(reply);

    if (process_still_exists) {
      n_entries++;
      continue;
    }
    
    //
    // Stop the child process and its threads
    //
    // TODO: put kill/wait/destroy in the main event loop and send sigkill when
    // sigterm hasn't done the trick.
    //

    err = kill(plp->processID, SIGTERM);
    if (err == -1) {
      isLogging_err("%s: Failed to kill process %d: %s\n", id, plp->processID, strerror(errno));
    } else {
      err = waitpid(plp->processID, NULL, WNOHANG);
      if (err == -1) {
        isLogging_err("%s: Failed to wait for process %d: %s\n", id, plp->processID, strerror(errno));
      }
    }

    // Go ahead and destroy this process;
    isDestroyProcessListItem(plp);
  }
  
  //isLogging_info("%s: found %d entries\n", id, n_entries);

  errno = 0;
  process_table_size = n_entries + INITIAL_PROCESS_TABLE_SIZE;
  err = hcreate_r(process_table_size, &process_table);
  if (err == 0) {
    isLogging_err("%s: Failed to remake hash table: %s\n", id, strerror(errno));
    exit (-1);
  }

  for (plp = firstProcessListItem; plp != NULL; plp = plp->next) {

    item.key  = (char *)plp->key;
    item.data = plp;
    errno = 0;
    err = hsearch_r(item, ENTER, &rtn_value, &process_table);
    if( err == 0) {
      isLogging_err("failed to rebuild process list table: %s\n", strerror(errno));
      exit (-1);
    }
  }
  isRemakeZMQPollItems(NULL, NULL, NULL);
}


/** Spew our process list for debugging.
 **
 */
void listProcesses() {
  static const char *id = FILEID "listProcesses";
  isProcessListType *pp;
  int i;

  for (i=1, pp=firstProcessListItem; pp!=NULL; i++, pp=pp->next) {
    isLogging_debug("%s: %d: %s\n", id, i, pp->key);
  }
}


/** Find a process by its pid and esaf number.
 **
 ** @param pid  PID of the user instance.
 **
 ** @param esaf ESAF number the user wishes to access.
 **
 ** @returns Process list item that matches or NULL if no match was
 ** found.
 */
isProcessListType *isFindProcess(const char *pid, int esaf) {
  char ourKey[128];
  isProcessListType *p;
  ENTRY item;
  ENTRY *item_return;
  int err;

  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d", pid, esaf);
  ourKey[sizeof(ourKey)-1] = 0;

  item.key = ourKey;
  item.data = NULL;
  item_return = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &item_return, &process_table);
  if (err == 0) {
    return NULL;
  }
  p = item_return->data;
  if (p == NULL) {
    isLogging_err("isFindProcesses: hsearch succeeded but returned null data for key %s\n", ourKey);
    listProcesses();
    return NULL;
  }
  return p;
}

/** Ensure the specified process is running assuming such a process is
 ** allowed.
 **
 ** @param zctx  ZMQ context for the sockets we be creating
 **
 ** @param rc Redis context for the local redis server
 **
 ** @param isAuth JSON object with our permissions
 **   @li @c isAuth.pid  Process ID associated with this user instance.
 **
 ** @param esaf ESAF number we want to use.
 **
 ** @returns Process list item for this, perhaps new, process.
 */
isProcessListType *isRun(void *zctx, redisContext *rc, json_t *isAuth, int esaf, int dev_mode) {
  static const char *id = FILEID "isRun";
  char ourKey[128];
  char *pid;
  isProcessListType *p;
  ENTRY item;
  ENTRY *item_return;
  int err;

  //
  // See if we already have this process going
  //
  pid = (char *)json_string_value(json_object_get(isAuth, "pid"));
  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d%s", pid, esaf, dev_mode ? "-dev" : "");
  ourKey[sizeof(ourKey)-1] = 0;

  item.key  = ourKey;
  item.data = NULL;
  item_return = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &item_return, &process_table);
  if (err != 0) {
    p = item_return->data;
    return p;
  }

  //
  // Process was not found
  //
  p = isCreateProcessListItem(zctx, isAuth, esaf, dev_mode);
  item.key  = (char *)p->key;
  item.data = p;

  errno = 0;
  err = hsearch_r(item, ENTER, &item_return, &process_table);
  p = item_return->data;
  if (err == 0) {
    isLogging_err("%s: Could not ENTER key %s: %s\n", id, p->key, strerror(errno));
    remakeProcessList(rc);
  }
  return p;
}
