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

void remakeProcessList() {
  ENTRY item;
  ENTRY *rtn_value;
  int err;
  isProcessListType *plp;
  int n_entries;
  
  fprintf(stderr, "remakeProcessList: starting remake\n");

  hdestroy_r(&worker_table);
  for (n_entries = 0, plp = firstProcessListItem; plp != NULL; plp = plp->next) {
    n_entries++;
  }
  
  fprintf(stderr, "remakeProcessList: found %d entries\n", n_entries);

  errno = 0;
  err = hcreate_r(n_entries + INITIAL_WORKER_TABLE_SIZE, &worker_table);
  if (err == 0) {
    fprintf(stderr, "Failed to remake hash table: %s\n", strerror(errno));
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

  isSupervisor(p);
}

isProcessListType *isCreateProcessListItem(json_t *isAuth, int esaf) {
  char ourKey[128];
  char *pid;
  isProcessListType *rtn;

  pid = (char *)json_string_value(json_object_get(isAuth, "pid"));

  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d", pid, esaf);
  ourKey[sizeof(ourKey)-1] = 0;

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "Out of memory (isCreateProcessListItme)\n");
    exit (-1);
  }

  rtn->key = strdup(ourKey);
  if (rtn->key == NULL) {
    fprintf(stderr, "isCreateProcessListItem: out of memory (rtn->key)\n");
    exit (-1);
  }
  rtn->processID = 0;
  rtn->esaf = esaf;
  rtn->isAuth = isAuth;
  json_incref(rtn->isAuth);
    
  rtn->do_not_call = 0;

  rtn->next = firstProcessListItem;
  firstProcessListItem = rtn;

  isStartProcess(rtn);
  return rtn;
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

void isProcessDoNotCall( const char *pid, int esaf) {
  //
  // Flag for our garbage collection routine.  When do_not_call is
  // true and we have no processes left under this pid then we
  // terminate the worker.
  //
  // TODO: write the garbage collector
  //

  char ourKey[128];
  ENTRY item;
  ENTRY *item_return;
  isProcessListType *p;
  int err;

  snprintf(ourKey, sizeof(ourKey)-1, "%s-%d", pid, esaf);
  ourKey[sizeof(ourKey)-1] = 0;

  item.key = ourKey;

  errno = 0;
  item_return = NULL;
  err = hsearch_r(item, FIND, &item_return, &worker_table);
  if (err != 0 && item_return != NULL) {
    p = item_return->data;
    p->do_not_call = 1;
  }
}

const char *isRun(json_t *isAuth, int esaf) {
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
    remakeProcessList();
  }
  return p->key;
}
