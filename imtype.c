#include "is.h"

char *extensions[] = { ".img", ".mccd", ".mar3450", ".mar3000", ".mar2400", ".mar1800", ".mar2300", ".mar2000", ".mar1600", ".mar1200"};

void typeDispatch() {
  int fd;		// file descriptor
  unsigned int f4;	// first 4 bytes in file
  unsigned short f2;	// first 2 byes in file
  unsigned char f1;	// first 1 byte in file
  int br;		// bytes read
  int foundit;		// flag indicating we figured out what the file type is
  imtype_type *imp;	// pointer to images parsing info
  int i;		// loop counter
  char tmpFilename[256];// used to find the real file name

  fd = -1;
  fd =  open( filename, O_RDONLY, 0);
  for( i=0; fd == -1 && i<sizeof( extensions)/sizeof( extensions[0]); i++) {
    strncpy( tmpFilename, filename, sizeof( tmpFilename)-1-strlen(extensions[i]));
    tmpFilename[sizeof( tmpFilename)-1] = 0;
    strcat( tmpFilename, extensions[i]);
    fd =  open( tmpFilename, O_RDONLY, 0);
    if( fd > -1) {
      strncpy( filename, tmpFilename, sizeof( filename)-1);
      filename[sizeof(filename)-1] = 0;
    }
  }
    

  if( fd < 0) {
    fprintf( stderr, "Could not open file %s\n", filename);
    exit( 1);
  }

  br = read( fd, (char *)&f4, 4);
  if( br != 4) {
    fprintf( stderr, "Could not read 4 bytes from file %s\n", filename);
    exit( 1);
  }

  lseek( fd, 0, SEEK_SET);
  br = read( fd, (char *)&f2, 2);
  if( br != 2) {
    fprintf( stderr, "Could not read 2 bytes from file %s\n", filename);
    exit( 1);
  }

  lseek( fd, 0, SEEK_SET);
  br = read( fd, (char *)&f1, 1);
  if( br != 1) {
    fprintf( stderr, "Could not read 1 bytes from file %s\n", filename);
    exit( 1);
  }
  close( fd);
  
  if( debug) {
    fprintf( stderr, "f1: %0x, f2: %0x, f4: %0x\n", f1, f2, f4);
  }

  foundit = 0;
  for( i=0; imTypeArray[i] != NULL; i++) {
    imp = imTypeArray[i];

    if( debug) {
      fprintf( stderr, "%s   %s%0x,  %s%0x, %s%0x\n", imp->name, (imp->useF1==1 ? "USE:":""), imp->f1, (imp->useF2==1 ? "USE:":""), imp->f2, (imp->useF4==1 ? "USE:":""), imp->f4);
    }

    if( imp->useF1==1) {if( f1 == imp->f1) { foundit = 1;} else { foundit = 0;}}
    if( imp->useF2==1) {if( f2 == imp->f2) { foundit = 1;} else { foundit = 0;}}
    if( imp->useF4==1) {if( f4 == imp->f4) { foundit = 1;} else { foundit = 0;}}
    if( foundit == 1)
      break;
  }

  if( foundit) {
    (imp->cnvrt)();
  } else {
    fprintf( stderr, "Unknown file type: %0x %0x %0x     %s\n", f1, f2, f4, filename);
  }
}
