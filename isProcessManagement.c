/*! @file isProcessManagement.c
 *  @copyright 2017 by Northwestern University All Rights Reserved
 *  @author Keith Brister
 *  @brief Process Management Code for the LS-CAT Image Server Version 2
 */
#include "is.h"

#define INITIAL_PROCESS_TABLE_SIZE 128
static int process_table_size;
static struct hsearch_data process_table;
static isProcessListType *firstProcessListItem = NULL;
static zmq_pollitem_t *isZMQPollItems = NULL;
static int n_processes;

void isInit() {
  static const char *id = FILEID "isInit";
  FILE *our_pid_file;
  pid_t old_pid;
  int nconv;
  int err;

  do {
    our_pid_file = fopen(PID_FILE_NAME, "r");
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
        fprintf(stderr, "%s: failed to read pid file: %s\n", id, strerror(errno));
      }
      break;
    }

    //
    // Go for the throat right off the bat. TODO: consider being
    // gentle at first to give the previous process a chance to clean
    // up.
    err = kill(-old_pid, 9);
    if (err == -1 && errno != ESRCH) {
      fprintf(stderr, "%s: Could not kill previous process: %s\n", id, strerror(errno));
      break;
    }
    
  } while (0);

  if (our_pid_file != NULL) {
    fclose(our_pid_file);
  }

  our_pid_file = fopen(PID_FILE_NAME, "w");
  if (our_pid_file == NULL) {
    fprintf(stderr, "%s: Could not open pid file: %s\n", id, strerror(errno));
    exit (-1);
  }

  fprintf(our_pid_file, "%d", (int)getpid());
  fclose(our_pid_file);
}

void isProcessListInit() {
  static const char *id = FILEID "isProcessListInit";
  int err;
  firstProcessListItem = NULL;

  process_table_size = INITIAL_PROCESS_TABLE_SIZE;
  errno = 0;
  err = hcreate_r(process_table_size, &process_table);
  if (err == 0) {
    fprintf(stderr, "%s: Fatal error: %s\n", id, strerror(errno));
    exit (-1);
  }
  n_processes = 0;
}

void isStartProcess(isProcessListType *p) {
  static const char *id = FILEID "isStartProcess";
  int child;
  struct passwd *pwds;
  struct passwd *esaf_pwds;
  int uid;
  int gid;
  const char *userName;
  const char *homeDirectory;
  int err;
  char esafUser[16];

  userName = json_string_value(json_object_get(p->isAuth, "uid"));

  errno = 0;
  pwds = getpwnam(userName);
  if (pwds == NULL) {
    fprintf(stderr, "%s: Could not start process for user %s: %s\n", id, userName, strerror(errno));
    return;
  }

  uid = pwds->pw_uid;

  if (p->esaf > 40000) {
    snprintf(esafUser, sizeof(esafUser)-1, "e%d", p->esaf);
    esafUser[sizeof(esafUser)-1] = 0;

    errno = 0;
    esaf_pwds = getpwnam(esafUser);
    if (esaf_pwds == NULL) {
      fprintf(stderr, "%s: bad esaf user name '%s':%s\n", id, esafUser, strerror(errno));
      return;
    }
    gid = esaf_pwds->pw_gid;
    homeDirectory = strdup(esaf_pwds->pw_dir);
  } else {
    gid = pwds->pw_gid;
    homeDirectory = strdup(pwds->pw_dir);
  }

  fprintf(stdout, "%s: Starting sub process: uid=%d, gid=%d, dir: %s,  User: %s  ESAF: %d\n", id, uid, gid, homeDirectory, userName, p->esaf);

  errno = 0;
  child = fork();
  if (child == -1) {
    fprintf(stderr, "%s: Could not start sub process: %s\n", id, strerror(errno));
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
    fprintf(stderr, "%s: Could not change gid to %d: %s\n", id, gid, strerror(errno));
    _exit (-1);
  }

  errno = 0;
  err = setuid(uid);
  if (err != 0) {
    fprintf(stderr, "Could not change uid to %d: %s\n", uid, strerror(errno));
    _exit (-1);
  }

  errno = 0;
  err = chdir(homeDirectory);
  if (err != 0) {
    fprintf(stderr, "%s: Could not change working directory to %s: %s\n", id, homeDirectory, strerror(errno));
    _exit (-1);
  }

  isSupervisor(p->key);
}

isProcessListType *isCreateProcessListItem(void *zctx, json_t *isAuth, int esaf) {
  static const char *id = FILEID "isCreateProcessListItem";
  char ourKey[128];
  char *pid;
  isProcessListType *rtn;
  char dealer_endpoint[256];
  int err;
  int socket_option;

  pid = (char *)json_string_value(json_object_get(isAuth, "pid"));

  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d", pid, esaf);
  ourKey[sizeof(ourKey)-1] = 0;

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->key = strdup(ourKey);
  if (rtn->key == NULL) {
    fprintf(stderr, "%s: out of memory (rtn->key)\n", id);
    exit (-1);
  }
  rtn->processID = 0;
  rtn->esaf = esaf;
  rtn->isAuth = isAuth;
  json_incref(rtn->isAuth);
    
  rtn->parent_dealer = zmq_socket(zctx, ZMQ_DEALER);
  if (rtn->parent_dealer == NULL) {
    fprintf(stderr, "%s: Could not create parent_dealer socket for %s: %s\n", id, rtn->key, zmq_strerror(errno));
    exit (-1);
  }
  
  socket_option = 0;
  err = zmq_setsockopt(rtn->parent_dealer, ZMQ_RCVHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set RCVWM for parent_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  socket_option = 0;
  err = zmq_setsockopt(rtn->parent_dealer, ZMQ_SNDHWM, &socket_option, sizeof(socket_option));
  if (err == -1) {
    fprintf(stderr, "%s: Could not set SNDWM for parent_dealer: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  snprintf(dealer_endpoint, sizeof(dealer_endpoint)-1, "ipc://@%s", rtn->key);
  dealer_endpoint[sizeof(dealer_endpoint)-1] = 0;

  err = zmq_bind(rtn->parent_dealer, dealer_endpoint);
  if (err == -1) {
    fprintf(stderr, "%s: Could not bind to socket %s: %s\n", id, dealer_endpoint, zmq_strerror(errno));
    exit (-1);
  }

  rtn->next = firstProcessListItem;
  firstProcessListItem = rtn;

  isRemakeZMQPollItems(NULL, NULL, NULL);

  isStartProcess(rtn);
  return rtn;
}

//
// Recreate (or initially create) the zmq pollitems we need in the main
// loop to service the zmq sockets on which we rely
//
// Call with parent_router to insert as the first item.  Set to null
// when remaking and it will not be touched.
//
// If we need to add other file descriptors to the event loop we'll
// need to add a mechanism to deal them.
//
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
    fprintf(stderr, "%s: Out of memory\n", id);
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

zmq_pollitem_t *isGetZMQPollItems() {
  return isZMQPollItems;
}

int isNProcesses() {
  return n_processes;
}

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
    fprintf(stderr, "%s: could not find process %s\n", id, p->key);
    return;
  }

  err = zmq_close(p->parent_dealer);
  if (err == -1) {
    fprintf(stderr, "%s: Could not close parent_dealer for %s: %s\n", id, p->key, zmq_strerror(errno));
  }

  p->parent_dealer = NULL;
  last->next = p->next;
  json_decref(p->isAuth);
  free((char *)p->key);
  free(p);

  isRemakeZMQPollItems(NULL, NULL, NULL);
}

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
  
  //fprintf(stdout, "%s: starting remake\n", id);

  hdestroy_r(&process_table);
  for (n_entries = 0, plp = firstProcessListItem; plp != NULL; plp = plp->next) {
    pid = json_string_value(json_object_get(plp->isAuth, "pid"));
    if (pid == NULL) {
      fprintf(stderr, "%s: Failed to find pid in process list\n", id);
      exit (-1);
    }
    // See if the pid still exists
    //
    reply = redisCommand(rc, "EXISTS %s", pid);
    if (reply == NULL) {
      fprintf(stderr, "%s: Redis error: %s\n", id, rc->errstr);
      exit (-1);
    }
    if (reply->type != REDIS_REPLY_INTEGER) {
      fprintf(stderr, "%s: redis exists returned unexpected type %d\n", id, reply->type);
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
      fprintf(stderr, "%s: Failed to kill process %d: %s\n", id, plp->processID, strerror(errno));
    } else {
      err = waitpid(plp->processID, NULL, WNOHANG);
      if (err == -1) {
        fprintf(stderr, "%s: Failed to wait for process %d: %s\n", id, plp->processID, strerror(errno));
      }
    }

    // Go ahead and destroy this process;
    isDestroyProcessListItem(plp);
  }
  
  //fprintf(stdout, "%s: found %d entries\n", id, n_entries);

  errno = 0;
  process_table_size = n_entries + INITIAL_PROCESS_TABLE_SIZE;
  err = hcreate_r(process_table_size, &process_table);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to remake hash table: %s\n", id, strerror(errno));
    exit (-1);
  }

  for (plp = firstProcessListItem; plp != NULL; plp = plp->next) {

    item.key  = (char *)plp->key;
    item.data = plp;
    errno = 0;
    err = hsearch_r(item, ENTER, &rtn_value, &process_table);
    if( err == 0) {
      fprintf(stderr, "failed to rebuild process list table: %s\n", strerror(errno));
      exit (-1);
    }
  }
  isRemakeZMQPollItems(NULL, NULL, NULL);
}


void listProcesses() {
  isProcessListType *pp;
  int i;

  for (i=1, pp=firstProcessListItem; pp!=NULL; i++, pp=pp->next) {
    fprintf(stdout, "listProcesses: %d: %s\n", i, pp->key);
  }
}

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
    fprintf(stderr, "isFindProcesses: hsearch succeeded but returned null data for key %s\n", ourKey);
    listProcesses();
    return NULL;
  }
  return p;
}

isProcessListType *isRun(void *zctx, redisContext *rc, json_t *isAuth, int esaf) {
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
  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d", pid, esaf);
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

  //  fprintf( stderr, "isRun: Could not find key %s: %s\n", ourKey, strerror(errno));
  //
  // Process was not found
  //
  p = isCreateProcessListItem(zctx, isAuth, esaf);
  item.key  = (char *)p->key;
  item.data = p;

  errno = 0;
  err = hsearch_r(item, ENTER, &item_return, &process_table);
  p = item_return->data;
  if (err == 0) {
    fprintf( stderr, "isRun: Could not ENTER key %s: %s\n", p->key, strerror(errno));
    remakeProcessList(rc);
  }
  return p;
}
