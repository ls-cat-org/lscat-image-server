#include "is.h"

//
// Global variables used in the modules that actually do the work
//
unsigned int xsize, ysize, wpixel, bpixel, jpq;
double zoom, xcen, ycen;
char filename[256];


static char usage[] = {
  "Usage:\n\
    is [options]\n\
This program converts diffraction images into a jpeg image for display.\n\
\n\
    Options:\n\
    -b  --black-pixel     value of original image to assign to black\n\
    -c  --contrast        same as -b or --black-pixel\n\
    -f  --file-name       name of marccd file\n\
    -h  --help            this message\n\
    -j  --jpeg-quality    quality of output (0-100)\n\
    -x  --x-center        center of output image (0.0 to 1.0)\n\
    -y  --y-center        center of output image (0.0 to 1.0)\n\
    -z  --zoom            level of zoom\n\
    -w  --white-pixel     value of original image to assign to white\n\
    -X  --x-size          width of output image in pixels\n\
    -Y  --y-size          length of output image in pixels\n\
"};


int main( int argc, char **argv) {
  static struct option long_options[] = {
    {"file-name",     1, 0, 'f'},
    {"x-size",        1, 0, 'X'},
    {"y-size",        1, 0, 'Y'},
    {"zoom",          1, 0, 'z'},
    {"contrast",      1, 0, 'c'},
    {"black-pixel",   1, 0, 'b'},
    {"x-center",      1, 0, 'x'},
    {"y-center",      1, 0, 'y'},
    {"white-pixel",   1, 0, 'w'},
    {"jpeg-quality",  1, 0, 'j'},
    {"help",          0, 0, 'h'},
    { 0, 0, 0, 0}
  };
  int option_index;

  int showUsage;
  int c;

  filename[0] =0;
  xsize  = 256;
  ysize  = 256;
  zoom   = 1.0;
  bpixel = 65535;
  wpixel = 0;
  jpq = 100;
  showUsage = 0;
  xcen=0.5;
  ycen=0.5;
  for( option_index=0, c=0; c != -1;
       c=getopt_long( argc, argv, "hf:X:Y:z:c:b:x:y:w:j:", long_options, &option_index)) {
    switch( c) {
    case 'f':
      strncpy( filename, optarg, sizeof( filename)-1);
      filename[sizeof(filename)-1]=0;
      break;
      
    case 'h':
      showUsage = 1;
      break;

    case 'X':
      xsize = atoi( optarg);
      break;

    case 'Y':
      ysize = atoi( optarg);
      break;

    case 'z':
      zoom = atof( optarg);
      break;

    case 'x':
      xcen = atof( optarg);
      break;

    case 'y':
      ycen = atof( optarg);
      break;

    case 'w':
      wpixel = atoi( optarg);
      break;

    case 'c':
    case 'b':
      bpixel = atoi( optarg);
      break;

    case 'j':
      jpq = atoi( optarg);
      break;
    }
    
  }

  if( showUsage || strlen( filename) <= 0) {
    fprintf( stderr, usage);
    exit( 1);
  }

  typeDispatch();


  exit( 0);
}
