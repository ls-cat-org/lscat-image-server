/*
** is.c
**
** LS-CAT image server command line support
**
** Copyright (C) 2009-2010 by Keith Brister
** All rights reserved.
**
*/
#include "is.h"

//
// Global variables used in the modules that actually do the work
//


static char usage[] = {
  "Copyright (C) 2009-2010 by Keith Brister. All rights reserved.\n\
    Usage:\n\
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

void ibInit( imBufType *ib) {
  //
  // clear the entire structure
  //
  memset( ib, 0, sizeof( imBufType));
  //
  ib->magic = IBMAGIC;
  if( pthread_rwlock_init( &(ib->headlock), NULL)) {
    fprintf( stderr, "ibInit: headlock init failed\n");
    exit( -1);
  }
  if( pthread_rwlock_init( &(ib->datalock), NULL)) {
    fprintf( stderr, "ibInit: datalock init failed\n");
    exit( -1);
  }
}

void imBufGarbageCollect( imBufType *ib) {
  if( ib->magic != IBMAGIC) {
    fprintf( stderr, "imBufGarbageCollect: corrupt image buffer!\n");
    exit( -1);
  }
  pthread_rwlock_wrlock( &(ib->headlock));	// lock the header
  pthread_rwlock_wrlock( &(ib->datalock));	// lock the data


  pthread_mutex_lock( &ibUseMutex);

  ib->dataRead = 0;
  ib->headerRead = 0;

  if( ib->nuse <= 0) {
    if( ib->fn != NULL) {
      free( ib->fn);
      ib->fn = NULL;
    }
    if( ib->h_filename != NULL) {
      free( ib->h_filename);
      ib->h_filename = NULL;
    }
    if( ib->h_dir != NULL) {
      free( ib->h_dir);
      ib->h_dir = NULL;
    }
    if( ib->fullbuf != NULL) {
      free( ib->fullbuf);
      ib->fullbuf = NULL;
      ib->buf     = NULL;
    }
  }
  pthread_mutex_unlock( &ibUseMutex);

  pthread_rwlock_unlock( &(ib->datalock));
  pthread_rwlock_unlock( &(ib->headlock));
}

//
// return an image buffer
//
imBufType *imBufGet( char *fn) {
  int i;
  int foundIt;
  int nuse;

  foundIt = 0;
  for( i=0; i<NIBUFS; i++) {
    pthread_mutex_lock( &ibUseMutex);
    if( ibs[i].fn != NULL && strcmp( ibs[i].fn, fn) == 0) {
      ibs[i].nuse++;			// someone else is using the buffer, better lock out nuse while we change it
      pthread_mutex_unlock( &ibUseMutex);
      return( &ibs[i]);
    }
    pthread_mutex_unlock( &ibUseMutex);
  }

  //
  // Here means we'll need to read the file:
  // find an unused buffer
  //
  for( i=0; i<NIBUFS; i++) {
    pthread_mutex_lock( &ibUseMutex);
    nuse = ibs[i].nuse;
    pthread_mutex_unlock( &ibUseMutex);
    if( nuse <=0) {
      imBufGarbageCollect( &ibs[i]);
      pthread_mutex_lock( &ibUseMutex);
      ibs[i].nuse = 1;			// At this point no other process is using this buffer, no locks needed
      ibs[i].fn = strdup( fn);
      pthread_mutex_unlock( &ibUseMutex);
      return( &ibs[i]);			// If someone else can allocate a buffer we'll need to add the locks
    }
  }
  //
  // Here means we don't have enough image buffers.  Perhaps NTHEADS > NIBUFS?
  // return null as an error and think about a different line of work.
  return( NULL);
}


//
// returns memory malloc'ed for the image structure
//  The corresponding create routine is dbGet
//
void isTypeDestroy( isType *is) {
  //
  // Don't mess with this structure until we know it's safe to
  //
  pthread_rwlock_wrlock( &isTypeChangeLock);

  if( is->magic == ISMAGIC) {
    is->nuse--;
    if( is->nuse <=0 ) {
      is->b = NULL;

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
      if( is->ifn1 != NULL) {
	free( is->ifn1);
	is->ifn1 = NULL;
      }
      if( is->ifn2 != NULL) {
	free( is->ifn2);
	is->ifn2 = NULL;
      }
      if( is->dspid != NULL) {
	free( is->dspid);
	is->dspid = NULL;
      }
    }
  }
  //
  // Don't mess with this structure until we know it's safe to
  //
  pthread_rwlock_unlock( &isTypeChangeLock);
}

void popIsQueue( isType *is) {
  int readNeeded;

  //
  // recover previously allocated space, if any
  //
  isTypeDestroy( is);

  sem_post( &workerSem);		// give up our semaphore

  pthread_mutex_lock( &workerMutex);	// grab the worker mutex to set up the signal

  //
  // Here we sit waiting for a request to come in
  //
  while( isQueueLength == 0) {				// Loop to eliminate spurious signals
    pthread_cond_wait( &workerCond, &workerMutex);	// wait for isDaemon to signal ready
  }
  
  sem_wait( &workerSem);		// grab a semaphore: isDaemon has already checked to make sure this will not hang

  //  
  // The actual work is a little anticlimatic
  // Just copy the next (only) item in the queue
  //
  memcpy( is, &isQueue, sizeof( isQueue));
  isQueueLength = 0;

  pthread_mutex_lock( &ibUseMutex);	// lock access to headerRead

  readNeeded = 0;
  if( is->b->headerRead == 0) {
    // We'll need to read the image header first: get write lock (to write to the buffer)
    pthread_rwlock_wrlock( &(is->b->headlock));
    is->b->headerRead = 1;		// Mark that it's read: this keeps a second thead from deciding to load the header data also
    readNeeded = 1;
  }

  pthread_mutex_unlock( &ibUseMutex);	// done messing with headerRead

  //
  // Allow other threads to start up
  //
  pthread_mutex_unlock( &workerMutex);

  //
  // At this point other workers can be, well, working
  // 
  if( readNeeded) {
    typeDispatch( is);				// find out what we are
    is->b->getHeader( is);				// Everyone needs the header
    pthread_rwlock_unlock( &(is->b->headlock));	// done loading the header into the buffer
  }
  //
  // NOW grab the read lock in case a different thread was doing the writing:
  // we don't want to go on until at least the header is there
  pthread_rwlock_rdlock( &(is->b->headlock));
}

void ibWorkerDone( isType *is) {
  // mark our lack of interest in this buffer
  pthread_mutex_lock( &ibUseMutex);
  is->b->nuse--;
  pthread_mutex_unlock( &ibUseMutex);

  // let the writers have their way
  //
  pthread_rwlock_unlock( &(is->b->headlock));
}

void cmdDispatch( isType *is) {
  if( strcmp( is->cmd, "jpeg") == 0) {
    ib2jpeg( is);
  } else if( strcmp( is->cmd, "profile") == 0) {
      ib2profile( is);
  } else if( strcmp( is->cmd, "header") == 0) {
    ib2header( is);
  } else if( strcmp( is->cmd, "download") == 0) {
    ib2download( is);
  } else if( strcmp( is->cmd, "indexing") == 0) {
    ib2indexing( is);
  } else if( strcmp( is->cmd, "tarball") == 0) {
    ib2tarball( is);
  }
}

void *worker( void *dummy) {
  struct hostent *hostInfo, hostInfoBuffer;
  int hostInfoErrno;
  char hostInfoAuxBuff[512];
  isType isInfo;
  struct sockaddr_in theAddr;
  int firstTime;

  //
  // decrement (wait) the semaphore before starting the loop
  // since the first thing we need to do is increment (post) it in popIsQueue
  //
  sem_wait( &workerSem);

  // initialize isInfo;
  memset( &isInfo, 0, sizeof( isType));

  // Don't try to unlock image buffer the first time out
  //
  firstTime = 1;

  while( 1) {
    //
    // Don't mess with isInfo the first time through the loop
    //
    if( !firstTime) {
      ibWorkerDone( &isInfo);
    } else {
      firstTime = 0;
    }

    popIsQueue( &isInfo); 

    gethostbyname_r( isInfo.ip, &hostInfoBuffer, hostInfoAuxBuff, sizeof(hostInfoAuxBuff), &hostInfo, &hostInfoErrno);

    if( hostInfo == NULL) {
      fprintf( stderr, "Couldn't find host %s\n", isInfo.ip);
      continue;
    }


    // Set up output socket
    isInfo.fd = socket( AF_INET, SOCK_STREAM, 0);
    if( isInfo.fd == -1) {
      fprintf( stderr, "socket call failed: %s\n", strerror( errno));
      continue;
    }
    
    theAddr.sin_family   = AF_INET;
    theAddr.sin_port     = htons( (unsigned short int)isInfo.port);
    memset( &theAddr.sin_addr.s_addr, 0, sizeof( theAddr.sin_addr.s_addr));
    theAddr.sin_addr.s_addr = ((struct in_addr *) hostInfo->h_addr_list[0])->s_addr;

    if( connect( isInfo.fd,(struct sockaddr *) &theAddr, sizeof( theAddr)) == -1) {
      fprintf( stderr, "connect to %s failed: %s\n", isInfo.ip, strerror( errno));
      continue;
    }

    isInfo.fout = fdopen( isInfo.fd, "w");
    if( isInfo.fout == NULL) {
      fprintf( stderr, "fdopen failed: %s\n", strerror( errno));
      continue;
    }

    //usleep( 400000);	// Kludge to wait for lustre to actually deliver the files

    cmdDispatch( &isInfo);
    if( isInfo.fd >= 0)
      close( isInfo.fd);
  }
  return 0;
}


int checkWhosUp() {
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

  is->b = imBufGet( is->fn);
  if( is->b == NULL) {
    fprintf( stderr, "pushIsQueue: received null image buffer.  I can't live with this shame.\n");
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
  struct stat sb;
  struct sigaction sigact;
  char **spp;
  FILE *pidfile;
#ifdef PROFILE
  int countdown;
#endif

  // Ignore SIGPIPE
  // When the connection dies it might happen
  // while writing causing a SIGPIPE which has the
  // default action of killing the program
  //
  sigact.sa_handler = SIG_IGN;
  sigemptyset( &sigact.sa_mask);
  sigact.sa_flags = 0;
  sigaction( SIGPIPE, &sigact, NULL);

  // put our pid into /var/run/ls-cat/is
  mkdir( "/var/run/ls-cat", 0777);
  pidfile = fopen( "/var/run/ls-cat/is.pid", "w");
  if( pidfile != NULL) {
    fprintf( pidfile, "%d", getpid());
    fclose( pidfile);
  }

  // initialize the database connection
  dbInit();

  // initialize the image buffers
  for( i=0; i<NIBUFS; i++) {
    ibInit( &(ibs[i]));
  }



  //
  // set up mutex, condition, and semaphore to handle thread synchronization
  //
  pthread_rwlock_init( &isTypeChangeLock, NULL);
  pthread_mutex_init( &ibUseMutex, NULL);
  pthread_mutex_init( &workerMutex, NULL);
  pthread_cond_init( &workerCond, NULL);
  sem_init( &workerSem, 0, NTHREADS);

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
    sem_wait( &workerSem);

    //
    // Give back the semaphore: we don't want it for ourselves
    //
    sem_post( &workerSem);

    dbWait();
    gotOne = dbGet( &isInfo);
    if( gotOne != 1)
      continue;

    if( stat( isInfo.fn, &sb) == -1) {
      fprintf( stderr, "stat error: %s on file '%s'\n", strerror( errno), isInfo.fn);
      continue;
    }

    if( sb.st_gid < 5000000) {
      fprintf( stderr, "gid too small for an LS-CAT image: %d\n", sb.st_gid);
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


    isInfo.gid = sb.st_gid;
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
    // We assume the this is a regular file owned by the esaf with group read permissions
    //
    if( (sb.st_mode & S_IFREG) && (sb.st_uid == isInfo.gid)  && (sb.st_mode & S_IRGRP)) {
      //
      // That's all we're going to do.  Let a child thread do the rest
      //
      pushIsQueue( &isInfo);
    } else {
      fprintf( stderr, "File read failed because the following:\n%s%s%s\n", (sb.st_mode & S_IFREG) ? "" : "   Not a regular file\n", (sb.st_uid == isInfo.gid) ? "":"   Wrong group ownership\n", (sb.st_mode & S_IRGRP) ? "":"   Not group readable\n");
    }
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
    cmdDispatch( &isInfo);
  }

  exit( 0);
}


