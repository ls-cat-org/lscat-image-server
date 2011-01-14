/*
** is.h
**
** Common includes for the LS-CAT image server
**
** Copyright (C) 2009-2010 by Keith Brister
** All rights reserved.
**
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <jpeglib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <tiffio.h>
#include <jpeglib.h>
#include <fcntl.h>
#include <math.h>
#include <libpq-fe.h>
#include <poll.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <sys/stat.h>
#include <sys/fsuid.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netdb.h>
#include <setjmp.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <wait.h>

#define NTHREADS 16
#define NIBUFS 16
#define ISQUEUESIZE 1
#define ISPADSIZE   128

//
// Magic numbers indicating initiallized memory
// Used to make plausable (but does not absolutely guaranty that an image structures are value
//
#define IBMAGIC 2009071701L
#define ISMAGIC 2009071702L

//
// Global variables used in the modules that actually do the work
//

typedef struct im_buf_struct {
  unsigned long magic;		// magic number to gaurd against using uninitialized memory
  pthread_rwlock_t headlock;	// Lock to keep bad things from happening: use to access everything EXCEPT data, nuse, headerRead, and dataRead
  pthread_rwlock_t datalock;	// Lock to keep bad things from happening: use to access data in buf or fullbuf
  int headerRead;		// 1 indicates header has been read, 0 it hasn't:  Protect with ibUseMutex
  int dataRead;			// 1 indicates data has been read, 0 it hasn't:    Protect with ibUseMutex
  struct is_struct *dummy;	// UNUSED except to make gcc happy about the is_strtuct pointers in the following structure parameter lists
  void (*getHeader)( struct is_struct *lala);	// function used to read header
  void (*getData)( struct is_struct *lala);	// function used to read data
  int nuse;			// number of users of this image
  char *fn;			// File name for this image
  int gid;			// gid of esaf owning this image
  int pad;			// rows and columns by which the image is padded
  unsigned int inWidth;		// width of the input image
  unsigned int inHeight;	// width of the input image
  //
  // The Header
  //
  char *h_filename;
  char *h_dir;
  float h_dist;
  float h_rotationRange;
  float h_startPhi;
  float h_wavelength;
  float h_beamX;
  float h_beamY;
  int   h_imagesizeX;
  int   h_imagesizeY;
  float h_pixelsizeX;
  float h_pixelsizeY;
  float h_integrationTime;
  float h_exposureTime;
  float h_readoutTime;
  int   h_saturation;
  int   h_minValue;
  int   h_maxValue;
  float h_meanValue;
  float h_rmsValue;
  int   h_nSaturated;
  unsigned short *fullbuf;	// actual buffer calloced
  unsigned short *buf;		// start of the raw image
} imBufType;

typedef struct is_struct {
  //
  // Structure housekeeping
  //
  unsigned long magic;		// the magic number 
  imBufType *b;			// Shared image buffer

  //
  // Request housekeeping
  //
  int nuse;
  char *user;			// user name to run as
  int uid;			// uid of user
  int gid;			// uid of the file
  int fd;			// file descriptor
  FILE *fout;			// output file pointer corresponding to fd
  char *rqid;			// client's request id
  int  esaf;			// esaf for the experiment
  char *cmd;			// command to run
  char *ip;			// ip address of waiting image servee
  int port;			// the port to connect to
  char *fn;			// the file requested
  //
  // jpeg parameters
  //
  int xsize;	// x size of final image
  int ysize;	// y size of final image
  int contrast;	// black pixel value
  int wval;	// white pixel value
  int x;	// x coordinate of upper left hand corner of original image segment
  int y;	// y coordinate of upper left hand corner of original image segment
  int width;	// width of original image segment
  int height;	// height of original image segment
  //
  // profile parameters
  //
  int pax;	// profile point a x coordinate
  int pay;	// profile point a y coordinate
  int pbx;	// profile point b x coordinate
  int pby;	// profile point b y coordinate
  int pw;	// profile width
  //
  // indexing filenames
  //
  char *ifn1;	// file name 1 for the indexing routine
  char *ifn2;	// file name 1 for the indexing routine
  //
  // Tarball parameter(s)
  //
  char *dspid;
} isType;


typedef struct kb_thread_struct {
  pthread_t thread;
  jmp_buf jumpHere;
} kbThreadType;


//
// structure to store image identification information
// the "use" numbers are flag indicating which tests to use
// the "f"'s are the number of bytes at the beginning of the file to look at
//
typedef struct imtype_struct {
  int useF1, useF2, useF4;
  unsigned char  f1;
  unsigned short f2;
  unsigned int   f4;
  char *name;
  void (*getHeader)( isType *is);
  void (*getData)( isType *is);
} imtype_type;

extern imtype_type *imTypeArray[];

/*
typedef struct adsc_header_struct {
  struct adsc_header_struct *next;
  char *key;
  char *val;
} adsc_header_type;
*/


//extern adsc_header_type *theHeader;

/*
typedef struct mar345_ascii_header_struct {
  struct mar345_ascii_header_struct *next;
  char *key;
  char *val;
} mar345_ascii_header_type;
*/
//extern mar345_ascii_header_type *theMar345AsciiHeader;
/*
typedef struct mar345_bin_header_struct {
  unsigned int swapFlag;	//  1) 1234 or else swap needed
  unsigned int n1Size;		//  2) image size in one dimension
  unsigned int nHi;		//  3) number of high intensity pixels
  unsigned int imageFormat;	//  4) 1=Compressed, 2=spiral
  unsigned int collectionMode;	//  5) 0=Dose, 1=time
  unsigned int nPixels;		//  6) Total pizels in image
  unsigned int pixelLength;	//  7) Pixel length (in microns)
  unsigned int pixelHeight;	//  8) Pixel height (in microns)
  unsigned int wavelength;	//  9) Wavelength (in micro-Angstorms)
  unsigned int distance;	// 10) Distance (in microns)
  unsigned int phiStart;	// 11) Starting phi (in milli-degrees)
  unsigned int phiEnd;		// 12) Ending phi (in milli-degrees)
  unsigned int omegaStart;	// 13) Starting Omega (in milli-degrees)
  unsigned int omegaEnd;	// 14) Ending Omega (in milli-degrees)
  unsigned int chi;		// 15) Chi (in milli-degrees)
  unsigned int twotheta;	// 16) 2-theta (in milli-degrees)
} mar345_bin_header_type;
*/
//extern mar345_bin_header_type theMar345BinHeader;

/*
typedef struct mar345_overflow_struct {
  unsigned int location;
  unsigned int value;
} mar345_overflow_type;
*/

extern void dbInit();
extern void dbWait();
extern int dbGet( isType *);


void typeDispatch( isType *is);
void marTiffGetHeader( isType *is);
void marTiffGetData( isType *is);
void ib2header( isType *is);
void ib2jpeg( isType *is);
void ib2profile( isType *is);
void ib2download( isType *is);
void ib2indexing( isType *is);
void ib2tarball( isType *is);
extern int debug;

extern sem_t workerSem;
extern pthread_rwlock_t isTypeChangeLock;
extern pthread_mutex_t ibUseMutex;
extern pthread_mutex_t workerMutex;
extern pthread_cond_t workerCond;
extern isType isQueue;
extern int isQueueLength;
extern pthread_t kbt[];
extern imBufType ibs[];


typedef struct cmdDispatchStruct {
  char *name;
  void (*func)( isType *);
} cmdDispatchType;

extern cmdDispatchType cmdDispatches[];
