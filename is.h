#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <tiffio.h>
#include <jpeglib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <tiffio.h>
#include <jpeglib.h>
#include <fcntl.h>
#include <png.h>

extern int debug;

//
// Global variables used in the modules that actually do the work
//
extern unsigned int xsize, ysize, wpixel, bpixel, jpq;
extern double zoom, xcen, ycen;
extern char filename[256];

void typeDispatch( void);
void marTiff2png( void);
void mar3452jpeg( void);
void adsc2jpeg( void);

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
  void (*cnvrt)();
} imtype_type;

extern imtype_type *imTypeArray[];


typedef struct adsc_header_struct {
  struct adsc_header_struct *next;
  char *key;
  char *val;
} adsc_header_type;



extern adsc_header_type *theHeader;


typedef struct mar345_ascii_header_struct {
  struct mar345_ascii_header_struct *next;
  char *key;
  char *val;
} mar345_ascii_header_type;

extern mar345_ascii_header_type *theMar345AsciiHeader;

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

extern mar345_bin_header_type theMar345BinHeader;


typedef struct mar345_overflow_struct {
  unsigned int location;
  unsigned int value;
} mar345_overflow_type;
