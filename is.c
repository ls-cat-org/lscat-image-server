#include "is.h"

//
// Global variables used in the modules that actually do the work
//


static char usage[] = {
  "Usage:\n\
    is [options]\n\
This program converts diffraction images into a jpeg image for display.\n\
\n\
    Options:\n\
    -c  --contrast        value of original image to assign to black\n\
    -d  --daemon          Daemon mode (all other parameters ignored)\n\
    -f  --file-name       name of marccd file\n\
    -h  --height          height of original image segment to map to output\n\
    -H  --y-size          length of output image in pixels\n\
    -w  --width           width of original image segment to map to output\n\
    -W  --x-size          width of output image in pixels\n\
    -p  --white-pixel     value of original image to assign to white\n\
    -x  --x-start         x coordinate value of upper left hand corner of original image\n\
    -y  --y-start         y coordinate value of upper left hand corner of original image\n\
    -?  --help            this message\n\
"};


void DieDieDie( ) {
  int i;

  for( i=0; i<NTHREADS; i++) {
    pthread_cancel( kbt[i]);
  }
  exit(-1);
}

//
// returns memory malloc'ed for the image structure
//  The corresponding create routine is dbGet
//
void isTypeDestroy( isType *is) {
  if( is->fullbuf != NULL) {
    free( is->fullbuf);
    is->fullbuf = NULL;
    is->buf     = NULL;
  }
  if( is->user != NULL) {
    free( is->user);
    is->user = NULL;
  }
  if( is->rqid != NULL) {
    free( is->rqid);
    is->rqid = NULL;
  }
  if( is->cmd != NULL) {
    free( is->cmd);
    is->cmd = NULL;
  }
  if( is->ip != NULL) {
    free( is->ip);
    is->ip = NULL;
  }
  if( is->fn != NULL) {
    free( is->fn);
    is->fn = NULL;
  }
}

void popIsQueue( isType *is) {
  
  if( sem_post( &workerSem) != 0) {
    fprintf( stderr, "popIsQueue: sem post failed\n");
    DieDieDie();
  }

  if( pthread_mutex_lock( &workerMutex) != 0) {
    fprintf( stderr, "popIsQueue: mutex lock failed\n");
    DieDieDie();
  }

  while( isQueueLength == 0) {
    if( pthread_cond_wait( &workerCond, &workerMutex) != 0) {
      fprintf( stderr, "popIsQueue: cond wait failed\n");
      DieDieDie();
    }
  }
  
  if( sem_wait( &workerSem) != 0) {
    fprintf( stderr, "popIsQueue: sem wait failed: %s\n", strerror( errno));
    DieDieDie();
  }
  
  memcpy( is, &isQueue, sizeof( isQueue));
  isQueueLength = 0;

  if( pthread_mutex_unlock( &workerMutex) != 0) {
    fprintf( stderr, "popIsQueue: mutex unlock failed\n");
    DieDieDie();
  }
}

void *worker( void *dummy) {
  struct hostent *hostInfo;
  isType isInfo;
  int outSoc;
  struct sockaddr_in theAddr;
  struct stat sb;

  //
  // decrement (wait) the semaphore before starting the loop
  // since the first thing we need to do is increment (post) it in popIsQueue
  //
  if( sem_wait( &workerSem) != 0) {
    fprintf( stderr, "popIsQueue: sem post failed\n");
    DieDieDie();
  }

  while( 1) {
    popIsQueue( &isInfo); 

    hostInfo = gethostbyname( isInfo.ip);
    if( hostInfo == NULL) {
      fprintf( stderr, "Couldn't find host %s\n", isInfo.ip);
      continue;
    }


    // Set up output socket
    outSoc = socket( AF_INET, SOCK_STREAM, 0);
    
    theAddr.sin_family   = AF_INET;
    theAddr.sin_port     = htons( (unsigned short int)isInfo.port);
    memset( &theAddr.sin_addr.s_addr, 0, sizeof( theAddr.sin_addr.s_addr));
    theAddr.sin_addr.s_addr = ((struct in_addr *) hostInfo->h_addr_list[0])->s_addr;

    if( connect( outSoc,(struct sockaddr *) &theAddr, sizeof( theAddr)) == -1) {
      fprintf( stderr, "connect to %s failed: %s\n", isInfo.ip, strerror( errno));
      continue;
    }

    if( setegid( isInfo.gid) == -1) {
      fprintf( stderr, "setgid to %d error: %s\n", isInfo.gid, strerror( errno));
      continue;
    }

    if( seteuid( isInfo.uid) == -1) {
      fprintf( stderr, "seteuid to %d error: %s\n", isInfo.uid, strerror( errno));
      continue;
    }

    fprintf( stderr, "Running as uid=%d, gid=%d\n", getuid(), getgid());
    fprintf( stderr, "Running as euid=%d, egid=%d\n", geteuid(), getegid());


    if( stat( isInfo.fn, &sb) == -1) {
      fprintf( stderr, "stat error: %s on file '%s'\n", strerror( errno), isInfo.fn);
      continue;
    }
    fprintf( stderr, "test  uid of file: %d\n", sb.st_uid);

    isInfo.fout = fdopen( outSoc, "w");
    typeDispatch( &isInfo);

    //write( outSoc, "Here I am\n", sizeof( "Here I am\n"));
    close( outSoc);


    if( seteuid( 0) == -1) {
      fprintf( stderr, "seteuid to %d error: %s\n", 0, strerror( errno));
      continue;
    }

    if( setegid( 0) == -1) {
      fprintf( stderr, "setgid to %d error: %s\n", 0, strerror( errno));
      continue;
    }
  }
  return 0;
}


[<int checkWhosUp() {
  int i;
  int rtn;

  rtn = 0;
  for( i=0; i<NTHREADS; i++) {
    if( pthread_kill( kbt[i], 0) != ESRCH)
      rtn++;
  }
  return rtn;
}

void pushIsQueue( isType *is) {
  int ntr;	// number of threads remaining

  //
  // Grab the lock
  //
  if( pthread_mutex_lock( &workerMutex) != 0) {
    fprintf( stderr, "pushIsQueue: mutex lock failed\n");
    DieDieDie();
  }
  //
  //  Queue should never need to be bigger.
  //
  memcpy( &isQueue, is, sizeof( isQueue));	// copy the image server structure
  isQueueLength = 1;				// note that there is a new entry in the queue


  //
  // Signal the worker threads that there is a new item in the queue
  //
  while( isQueueLength > 0) {
    if( pthread_cond_signal( &workerCond) != 0) {
      fprintf( stderr, "pushIsQueue: cond signal failed\n");
      DieDieDie();
    }

    //
    // Return the lock
    //
    if( pthread_mutex_unlock( &workerMutex) != 0) {
      fprintf( stderr, "pushIsQueue: mutex unlock failed\n");
      DieDieDie();
    }

    //
    // Grab the lock again: at least one of the waiting threads should have started by now:
    // we take the lock and check the queue length
    //
    if( pthread_mutex_lock( &workerMutex) != 0) {
      fprintf( stderr, "pushIsQueue: mutex lock failed\n");
      DieDieDie();
    }

    //
    // if the queue is still non zero we need to check that all threads are still running
    // as we'll be waiting forever if we have the thread count (and therefore the initial semaphore value) wrong
    //
    if( isQueueLength != 0 && (ntr = checkWhosUp()) != NTHREADS) {
      fprintf( stderr, "pushIsQueue: only found %d of the required %d threads.  Bad.  Very bad.\n", ntr, NTHREADS);
      DieDieDie();
    }
  }


  //
  // Finally we are done.  Return the lock one last time
  //
  if( pthread_mutex_unlock( &workerMutex) != 0) {
    fprintf( stderr, "pushIsQueue: mutex unlock failed\n");
    DieDieDie();
  }

}



void isDaemon() {
  int i;
  int ntr;
  isType isInfo;
  int gotOne;
  int foundUidFlag;
  struct passwd *pwInfo;
  struct group *grInfo;

  char **spp;
#ifdef PROFILE
  int countdown;
#endif

  dbInit();

  //
  // set up mutex, condition, and semaphore to handle thread synchronization
  //
  if( pthread_mutex_init( &workerMutex, NULL) != 0) {
    fprintf( stderr, "isDaemon: mutex init failed: %s\n", strerror( errno));
    exit(1);
  }

  if( pthread_cond_init( &workerCond, NULL) != 0) {
    fprintf( stderr, "isDaemon: cond init failed: %s\n", strerror( errno));
    exit(1);
  }

  if( sem_init( &workerSem, 0, NTHREADS) != 0) {
    fprintf( stderr, "isDaemon: sem init failed: %s\n", strerror( errno));
    exit(1);
  }

  //
  // Launch the threads
  //
  for( i=0; i<NTHREADS; i++) {
    pthread_create( &(kbt[i]), NULL, worker, NULL);
  }

  
#ifdef PROFILE
  for( countdown=16; countdown>0; countdown--) {
#else
  while( 1) {
#endif

    //
    // Check that the threads are all running: bad things happen if one or more is dead
    //
    if( (ntr = checkWhosUp()) != NTHREADS) {
      fprintf( stderr, "isDaemon: only found %d of the required %d threads.  Bad.  Very bad.\n", ntr, NTHREADS);
      DieDieDie();
    }

    //
    // Check the semaphore to be sure there is at least 1 thread willing to run for us
    //
    if( sem_wait( &workerSem) != 0) {
      fprintf( stderr, "isDaemon: sem wait failed: %s\n", strerror( errno));
      DieDieDie();
    }
    //
    // Give back the semaphore: we don't want it for ourselves
    //
    if( sem_post( &workerSem) != 0) {
      fprintf( stderr, "isDaemon: sem wait failed: %s\n", strerror( errno));
      DieDieDie();
      DieDieDie();
    }

    dbWait();
    gotOne = dbGet( &isInfo);
    if( gotOne != 1)
      continue;

    if( isInfo.esaf < 50000) {
      fprintf( stderr, "esaf too small: %d\n", isInfo.esaf);
      continue;
    }


    pwInfo = getpwnam( isInfo.user);
    if( pwInfo == NULL) {
      fprintf( stderr, "Null pwd info\n");
      continue;
    }
    if( pwInfo->pw_uid < 10000) {
      fprintf( stderr, "uid too low: %d\n", pwInfo->pw_uid);
      continue;
    }
    isInfo.uid = pwInfo->pw_uid;
    fprintf( stderr, "uid: %d    guid: %d  real name: %s\n", pwInfo->pw_uid, pwInfo->pw_gid, pwInfo->pw_gecos);


    isInfo.gid = isInfo.esaf * 100;
    grInfo = getgrgid( isInfo.gid);
    if( grInfo == NULL) {
      continue;
    }

    foundUidFlag = 0;
    for( spp = grInfo->gr_mem; *spp != NULL; spp++) {
      if( strcmp( *spp, isInfo.user) == 0) {
	foundUidFlag = 1;
	break;
      }
    }

    if( !foundUidFlag) {
      fprintf( stderr, "user %s not found in group %s\n", isInfo.user, grInfo->gr_name);
      continue;
    }


    //
    // That's all we're going to do.  Let a child thread do the rest
    //
    pushIsQueue( &isInfo);

  }
}



int main( int argc, char **argv) {

  static struct option long_options[] = {
    {"contrast",      1, 0, 'c'},
    {"daemon",        0, 0, 'd'},
    {"file-name",     1, 0, 'f'},
    {"height",        1, 0, 'h'},
    {"x-size",        1, 0, 'W'},
    {"x-start",       1, 0, 'x'},
    {"y-size",        1, 0, 'H'},
    {"y-start",       1, 0, 'y'},
    {"white-pixel",   1, 0, 'p'},
    {"width",         1, 0, 'w'},
    {"help",          0, 0, '?'},
    { 0, 0, 0, 0}
  };
  isType isInfo;
  int option_index;
  int showUsage;
  int daemonMode;
  int c;
  char filename[256];

  filename[0] =0;
  isInfo.height = 4096;
  isInfo.width  = 4096;
  isInfo.xsize  = 256;
  isInfo.ysize  = 256;
  isInfo.x      = 0;
  isInfo.y       = 0;
  isInfo.contrast = 65535;
  isInfo.wval = 0;
  showUsage = 0;
  daemonMode = 0;

  for( option_index=0, c=0; c != -1;
       c=getopt_long( argc, argv, "c:f:h:W:x:H:y:p:w:?d", long_options, &option_index)) {
    switch( c) {
    case 'f':
      strncpy( filename, optarg, sizeof( filename)-1);
      filename[sizeof(filename)-1]=0;
      isInfo.fn = filename;
      break;
      
    case '?':
      showUsage = 1;
      break;

    case 'W':
      isInfo.xsize = atoi( optarg);
      break;

    case 'H':
      isInfo.ysize = atoi( optarg);
      break;

    case 'x':
      isInfo.x = atoi( optarg);
      break;

    case 'y':
      isInfo.y = atoi( optarg);
      break;

    case 'w':
      isInfo.width = atoi( optarg);
      break;

    case 'h':
      isInfo.height = atoi( optarg);
      break;

    case 'c':
      isInfo.contrast  = atoi( optarg);
      break;

    case 'p':
      isInfo.wval = atoi( optarg);
      break;

    case 'd':
      daemonMode = 1;
      break;

    }
    
  }

  if( showUsage || (daemonMode==0 && strlen( filename) <= 0)) {
    fprintf( stderr, "%s\n", usage);
    exit( 1);
  }

  if( daemonMode) {
    isDaemon();
  } else {
    isInfo.fout = stdout;
    typeDispatch( &isInfo);
  }

  exit( 0);
}
