#include "is.h"

#define INITIAL_WORKER_TABLE_SIZE 128
static int worker_table_size;
static struct hsearch_data worker_table;
static isProcessListType *firstProcessListItem = NULL;

void isProcessListInit() {
  int err;
  firstProcessListItem = NULL;

  worker_table_size = INITIAL_WORKER_TABLE_SIZE;
  errno = 0;
  err = hcreate_r(worker_table_size, &worker_table);
  if (err == 0) {
    fprintf(stderr, "Fatal error: %s\n", strerror(errno));
    exit (-1);
  }
}

void isStartProcess(isProcessListType *p) {
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
    fprintf(stderr, "Could not start process for user %s: %s\n", userName, strerror(errno));
    return;
  }

  uid = pwds->pw_uid;
  if (p->esaf > 40000) {
    snprintf(esafUser, sizeof(esafUser)-1, "e%d", p->esaf);
    esafUser[sizeof(esafUser)-1] = 0;

    errno = 0;
    esaf_pwds = getpwnam(esafUser);
    if (esaf_pwds == NULL) {
      fprintf(stderr, "isStartProcess: bad esaf user name '%s':%s\n", esafUser, strerror(errno));
      return;
    }
    gid = esaf_pwds->pw_gid;
    homeDirectory = strdup(esaf_pwds->pw_dir);
  } else {
    gid = pwds->pw_gid;
    homeDirectory = strdup(pwds->pw_dir);
  }

  fprintf(stdout, "Starting sub process: uid=%d, gid=%d, dir: %s,  User: %s\n", uid, gid, homeDirectory, userName);

  errno = 0;
  child = fork();
  if (child == -1) {
    fprintf(stderr, "Could not start sub process: %s\n", strerror(errno));
    exit (-1);
  }
  if (child) {
    //
    // In parent.
    //
    p->processID = child;
    return;
  }
  //
  // In child
  //

  errno = 0;
  err = setgid(gid);
  if (err != 0) {
    fprintf(stderr, "Could not change gid to %d: %s\n", gid, strerror(errno));
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
    fprintf(stderr, "Could not change working directory to %s: %s\n", homeDirectory, strerror(errno));
    _exit (-1);
  }

  isSupervisor(p->key);
}

isProcessListType *isCreateProcessListItem(json_t *isAuth, int esaf) {
  static const char *id = FILEID "isCreateProcessListItem";
  char ourKey[128];
  char *pid;
  isProcessListType *rtn;

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
    
  rtn->next = firstProcessListItem;
  firstProcessListItem = rtn;

  isStartProcess(rtn);
  return rtn;
}

void isDestroyProcessListItem(isProcessListType *p) {
  static const char *id = "isDestroyProcessListItem";
  isProcessListType *plp, *last;

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

  last->next = p->next;
  json_decref(p->isAuth);
  free((char *)p->key);
  free(p);
}

void remakeProcessList(redisContext *rc, redisContext *rcLocal) {
  static const char *id = FILEID "remakeProcessList";
  ENTRY item;
  ENTRY *rtn_value;
  int err;
  isProcessListType *plp;
  int n_entries;
  const char *pid;
  redisReply *reply;
  int i;
  int process_still_exists;
  
  fprintf(stderr, "%s: starting remake\n", id);

  hdestroy_r(&worker_table);
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
    
    // Here we tell the threads to stop running. Using rpush instead
    // of lpush means that these are the next commands to be run even
    // if other things are in the queue.
    //
    // Threads are not allowed to pop anything else after recieving
    // the "end" command so we just push "end" as many times as we
    // have processes.

    for(i=0; i<N_WORKER_THREADS; i++) {
      redisCommand(rcLocal, "RPUSH %s end", plp->key);
    }

    // Go ahead and destroy this process;
    isDestroyProcessListItem(plp);
  }
  
  fprintf(stderr, "%s: found %d entries\n", id, n_entries);

  errno = 0;
  err = hcreate_r(n_entries + INITIAL_WORKER_TABLE_SIZE, &worker_table);
  if (err == 0) {
    fprintf(stderr, "%s: Failed to remake hash table: %s\n", id, strerror(errno));
    exit (-1);
  }

  for (plp = firstProcessListItem; plp != NULL; plp = plp->next) {

    item.key  = (char *)plp->key;
    item.data = plp;
    errno = 0;
    err = hsearch_r(item, ENTER, &rtn_value, &worker_table);
    if( err == 0) {
      fprintf(stderr, "failed to rebuild process list table: %s\n", strerror(errno));
      exit (-1);
    }
  }
}


void listProcesses() {
  isProcessListType *pp;
  int i;

  for (i=1, pp=firstProcessListItem; pp!=NULL; i++, pp=pp->next) {
    fprintf(stdout, "listProcesses: %d: %s\n", i, pp->key);
  }
}

const char *isFindProcess(const char *pid, int esaf) {
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
  err = hsearch_r(item, FIND, &item_return, &worker_table);
  if (err == 0) {
    return NULL;
  }
  p = item_return->data;
  if (p == NULL) {
    fprintf(stderr, "isFindProcesses: hsearch succeeded but returned null data for key %s\n", ourKey);
    listProcesses();
    return NULL;
  }
  return p->key;
}

const char *isRun(redisContext *rc, redisContext *rcLocal, json_t *isAuth, int esaf) {
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
  err = hsearch_r(item, FIND, &item_return, &worker_table);
  if (err != 0) {
    p = item_return->data;
    return p->key;
  }

  //  fprintf( stderr, "isRun: Could not find key %s: %s\n", ourKey, strerror(errno));
  //
  // Process was not found
  //
  p = isCreateProcessListItem(isAuth, esaf);
  item.key  = (char *)p->key;
  item.data = p;

  errno = 0;
  err = hsearch_r(item, ENTER, &item_return, &worker_table);
  p = item_return->data;
  if (err == 0) {
    fprintf( stderr, "isRun: Could not ENTER key %s: %s\n", p->key, strerror(errno));
    remakeProcessList(rc, rcLocal);
  }
  return p->key;
}
