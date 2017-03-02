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
  int uid;
  int gid;
  const char *userName;
  const char *homeDirectory;
  int err;

  userName = json_string_value(json_object_get(p->isAuth_obj, "uid"));

  errno = 0;
  pwds = getpwnam(userName);
  if (pwds == NULL) {
    fprintf(stderr, "Could not start process for user %s: %s\n", userName, strerror(errno));
    return;
  }

  uid = pwds->pw_uid;
  gid = pwds->pw_gid;
  homeDirectory = strdup(pwds->pw_dir);

  fprintf(stdout, "Starting sub process: uid=%d, gid=%d, dir: %s,  User: %s\n", uid, gid, homeDirectory, userName);

  errno = 0;
  err = pipe2(p->req_pipe, O_NONBLOCK);
  //  err = pipe2(p->req_pipe, 0);
  if (err != 0) {
    fprintf(stderr, "Could not open pipes: %s\n", strerror(errno));
    exit (-1);
  }

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
    close(p->req_pipe[0]);                      // close the unused read end
    p->req_fout = fdopen(p->req_pipe[1],"w");   // need a FILE pointer for json_dumpf

    return;
  }
  //
  // In child
  //
  close(p->req_pipe[1]);           // close unused write end

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

isProcessListType *isCreateProcessListItem(json_t *isAuth_obj) {
  isProcessListType *rtn;

  rtn = calloc(1, sizeof(*rtn));
  if (rtn == NULL) {
    fprintf(stderr, "Out of memory (isCreateProcessListItme)\n");
    exit (-1);
  }

  fprintf(stderr, "   Creating process list with key %s\n", json_string_value(json_object_get(isAuth_obj,"pid")));

  rtn->key = json_string_value(json_object_get(isAuth_obj, "pid"));
  rtn->processID = 0;
  rtn->isAuth_obj = isAuth_obj;
  json_incref(rtn->isAuth_obj);
    
  rtn->job_on = 0;
  rtn->job_off = 0;
  rtn->do_not_call = 0;
  pthread_mutex_init(&rtn->job_mutex, NULL);
  pthread_cond_init( &rtn->job_cond, NULL);

  rtn->next = firstProcessListItem;
  firstProcessListItem = rtn;

  isStartProcess(rtn);

  {
    isProcessListType *pp;
    int i;

    for (i=1, pp = firstProcessListItem; pp != NULL; pp = pp->next, i++) {
      fprintf(stderr, "isCreateProcessListItem: item %d: %s\n", i, pp->key);
    }
  }


  return rtn;
}

int isHasProcess(const char *pid) {
  ENTRY item;
  ENTRY *item_return;
  int err;

  item.key = (char *)pid;

  item_return = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &item_return, &worker_table);

  if (err == 0) {
    return 0;
  }

  return 1;
}

void isProcessDoNotCall( const char *pid) {
  //
  // Flag for our garbage collection routine.  When do_not_call is
  // true and we have no processes left under this pid then we
  // terminate the worker.
  //
  // TODO: write the garbage collector
  //

  ENTRY item;
  ENTRY *item_return;
  isProcessListType *p;
  int err;

  item.key = (char *)pid;

  errno = 0;
  item_return = NULL;
  err = hsearch_r(item, FIND, &item_return, &worker_table);
  if (err != 0 && item_return != NULL) {
    p = item_return->data;
    p->do_not_call = 1;
  }
}

void isRun(json_t *isAuth_obj, json_t *isRequest) {
  isProcessListType *p;
  ENTRY item;
  ENTRY *item_return;
  int err;
  char *jobstr;

  //
  // See if we already have this process going
  //
  item.key  = (char *)json_string_value(json_object_get(isRequest, "pid"));
  item.data = NULL;
  item_return = NULL;
  errno = 0;
  err = hsearch_r(item, FIND, &item_return, &worker_table);
  if (err != 0) {
    p = item_return->data;
    fprintf(stderr, "isRun: Found key %s\n", p->key);
  }

  if (err == 0) {
    fprintf( stderr, "isRun: Could not find key %s: %s\n", item.key, strerror(errno));

    if (isAuth_obj == NULL) {
      fprintf(stderr, "isRun: Cannot start new process without isAuth\n");
      return;
    }

    //
    // Process was not found
    //
    p = isCreateProcessListItem(isAuth_obj);
    item.key  = (char *)p->key;
    item.data = p;
    item_return = NULL;
    errno = 0;
    err = hsearch_r(item, ENTER, &item_return, &worker_table);
    if (err == 0) {
      fprintf( stderr, "isRun: Could not ENTER key %s: %s\n", item.key, strerror(errno));
      item_return = &item;
      remakeProcessList();
    }
  }

  //
  // If I understand the hsearch man page correctly then item_return
  // should point to our desired process regardless of which path we
  // took to get here.
  //
  p = item_return->data;

  jobstr = json_dumps(isRequest, JSON_COMPACT | JSON_INDENT(0));
  fprintf(p->req_fout, "%s\n", jobstr);
  fprintf(stdout, "queued job to process %d: %s\n", p->processID, jobstr);
  free(jobstr);
}
