#include "is.h"

//
// Global variables used in the modules that actually do the work
//
unsigned int xsize, ysize, wpixel, bpixel, jpq, xstart, ystart, height, width;
double zoom, xcen, ycen;
char filename[256];


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




void isDaemon() {
  isType isInfo;
  int gotOne;
  struct passwd *pwInfo;
  uid_t oldUid;
  struct stat sb;

  dbInit();
  
  while( 1) {
    dbWait();
    gotOne = dbGet( &isInfo);
    if( gotOne != 1)
      continue;

    if( isInfo.esaf < 60000) {
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
    fprintf( stderr, "uid: %d    guid: %d  real name: %s\n", pwInfo->pw_uid, pwInfo->pw_gid, pwInfo->pw_gecos);


    if( setfsuid( 82825) == -1) {
      fprintf( stderr, "seteuid to %d error: %s\n", 82825, strerror( errno));
      continue;
    }
    /*
    if( setfsuid( pwInfo->pw_uid) == -1) {
      fprintf( stderr, "seteuid to %d error: %s\n", pwInfo->pw_uid, strerror( errno));
      continue;
    }
    */
    if( setfsgid( isInfo.esaf * 100) == -1) {
      fprintf( stderr, "setgid to %d error: %s\n", isInfo.esaf*100, strerror( errno));
      continue;
    }

    fprintf( stderr, "Running as uid=%d, gid=%d\n", getuid(), getgid());
    fprintf( stderr, "Running as euid=%d, egid=%d\n", geteuid(), getegid());


    if( stat( isInfo.fn, &sb) == -1) {
      fprintf( stderr, "stat error: %s on file '%s'\n", strerror( errno), isInfo.fn);
      continue;
    }
    fprintf( stderr, "test  uid of file: %d\n", sb.st_uid);

    if( setegid( 0) == -1) {
      fprintf( stderr, "setgid to %d error: %s\n", 0, strerror( errno));
      continue;
    }

    if( seteuid( 0) == -1) {
      fprintf( stderr, "seteuid to %d error: %s\n", 0, strerror( errno));
      continue;
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
  int option_index;

  int showUsage;
  int daemonMode;
  int c;

  filename[0] =0;
  height = 4096;
  width  = 4096;
  xsize  = 256;
  ysize  = 256;
  xstart = 0;
  ystart = 0;
  bpixel = 65535;
  wpixel = 0;
  showUsage = 0;
  daemonMode = 0;

  for( option_index=0, c=0; c != -1;
       c=getopt_long( argc, argv, "c:f:h:W:x:H:y:p:w:?d", long_options, &option_index)) {
    switch( c) {
    case 'f':
      strncpy( filename, optarg, sizeof( filename)-1);
      filename[sizeof(filename)-1]=0;
      break;
      
    case '?':
      showUsage = 1;
      break;

    case 'W':
      xsize = atoi( optarg);
      break;

    case 'H':
      ysize = atoi( optarg);
      break;

    case 'x':
      xstart = atoi( optarg);
      break;

    case 'y':
      ystart = atoi( optarg);
      break;

    case 'w':
      width = atoi( optarg);
      break;

    case 'h':
      height = atoi( optarg);
      break;

    case 'c':
      bpixel = atoi( optarg);
      break;

    case 'p':
      wpixel = atoi( optarg);
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

  if( daemonMode)
    isDaemon();
  else
    typeDispatch();

  exit( 0);
}
