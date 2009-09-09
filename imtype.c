#include "is.h"

char *extensions[] = { ".img", ".mccd", ".mar3450", ".mar3000", ".mar2400", ".mar1800", ".mar2300", ".mar2000", ".mar1600", ".mar1200"};

void typeDispatch( isType *is) {
  int fd;		// file descriptor
  unsigned int f4;	// first 4 bytes in file
  unsigned short f2;	// first 2 byes in file
  unsigned char f1;	// first 1 byte in file
  int br;		// bytes read
  int foundit;		// flag indicating we figured out what the file type is
  imtype_type *imp;	// pointer to images parsing info
  int i;		// loop counter

  fd = -1;
  fd =  open( is->fn, O_RDONLY, 0);

  if( fd < 0) {
    fprintf( stderr, "Could not open file %s\n", is->fn);
    exit( 1);
  }

  br = read( fd, (char *)&f4, 4);
  if( br != 4) {
    fprintf( stderr, "Could not read 4 bytes from file %s\n", is->fn);
    exit( 1);
  }

  if( lseek( fd, 0, SEEK_SET) < 0L) {
    fprintf( stderr, "lseek 4 failed:  %s\n   %s\n", is->fn, strerror( errno));
    exit( 1);
  }

  br = read( fd, (char *)&f2, 2);
  if( br != 2) {
    fprintf( stderr, "Could not read 2 bytes from file %s\n", is->fn);
    exit( 1);
  }

  if( lseek( fd, 0, SEEK_SET) < 0L) {
    fprintf( stderr, "lseek 2 failed:  %s\n   %s\n", is->fn, strerror( errno));
    exit( 1);
  }

  br = read( fd, (char *)&f1, 1);
  if( br != 1) {
    fprintf( stderr, "Could not read 1 bytes from file %s\n", is->fn);
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
    is->b->getHeader = imp->getHeader;
    is->b->getData   = imp->getData;
  } else {
    fprintf( stderr, "Unknown file type: %0x %0x %0x     %s\n", f1, f2, f4, is->fn);
  }
}
