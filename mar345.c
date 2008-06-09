#include "is.h"
/*
One image consists of the following items:

Header record: 4096 bytes.




Optional high intensity records: 64 bytes each.  Pixels with values >
65535 are stored as pairs of address and actual pixel value. The pixel
address is a number corresponding to the linear array starting at
pixel 1 and ending at pixel SIZE*SIZE. Both address and value are
stored as 32 bit integers. One "high intensity" record thus consists
of 8 value pairs (address+intensity). Example: if an image contains 13
values > 65535, there will be 2 high intensity records of 8 value
pairs each ( 8*(4b+4b) = 64 bytes). Pairs 14 to 16 will contain zeroes
only. The total number of high intensity has to be taken from the
header. The number of high intensity records is calculated as:
HIGH_RECORDS = (int)( HIGH_PIXELS/8.0 + 0.875 ) where HIGH_PIXELS
denotes the total number of high intensity pixels.



A compressed image data array of variable length. All information is
stored in single bytes, so there is no need for byte swapping when
moving pck images in between different computer architectures (with
exception of the optional high intensity records). There is no loss of
information involved in compression. The compressed images take
approx. 70% less disk space. After decompression, a linear image data
array of SIZE records with SIZE pixels each is obtained. All pixels
values are 16-bit unsigned integers with values ranging from 0 to
65535. The first value in the array is in the upper left corner, the
fast varying axis is horizontal. This convention differs from the 300
mm mar by a clockwise rotation of 90.0 deg.!

SIZE denotes the no. of pixels in one dimension, e.g. 1200 or
3450. Each record is 2*SIZE bytes long.






Mar345 Header

The image header consists of "lines" of 64 bytes each with exception
of the first line, that contains 16 4-byte words (32 bit
integers). The very first value always is 1234 for all scanning modes,
i.e. it is just a marker for the necessity of byte swapping and does
NOT give the size of the image (in contrast to the first value of the
old image formats which is 1200 or 2000 or its byte swapped
equivalent). Programs of the mar345 suite rely on this value to decide
wether the image is in old format or in the new one and/or if
byte-swapping is required. The remaining 15 32-bit integers are:


2) Size of image in one dimension
3) Number of high intensity pixels
4) Image format (1=COMPRESSED, 2=SPIRAL)
5) Collection mode (0=DOSE, 1=TIME)
6) Total number of pixels in image
7) Pixel length (in mm * 1.000)
8) Pixel height (in mm * 1.000)
9) Used wavelength (in Ang * 1.000.000 )
10) Used distance (in mm * 1.000 )
11) Used starting PHI (in deg. * 1.000 )
12) Used ending PHI (in deg. * 1.000 )
13) Used starting OMEGA (in deg. * 1.000 )
14) Used ending OMEGA (in deg. * 1.000 )
15) Used CHI (in deg. * 1.000 )
16) Used TWOTHETA (in deg. * 1.000 )
*/

/*




If the first value is not 1234, the bytes of the following 15 integers
must be swapped.

The next 64 character line contains a general identifier string for
this type of file: "mar research" (bytes 65 to 76 in the image
file). This is for possible use with the "file" command under Unix.

All following lines contain keyworded information. The last keyword
should be "END OF HEADER". All keywords are in capital letters and all
"lines" are pure ASCII, so they are not affected by the byte order of
different computer platforms. Processing of the keywords is not
required. For using the formats correctly, the most important
information is contained in the second (bytes 5-8) and third (bytes
9-12) header value: the size of the image and the number of high
intensity pixels!



KEYWORDS in MAR345 HEADER

PROGRAM <program name> <version number>
Keyword PROGRAM always is the first in the list of keywords, i.e. at
byte 128!

DATE <weekday month day hh:mm:ss year>
Date and time of production. 
Example: DATE Tue Jul 9 13:06:05 1996

SCANNER <serial_number>
Serial number of the scanner. 
Example: SCANNER 12

FORMAT <size> <type> <no_pixels>
<size> is the number of pixels in one dimension, <type> is "MAR345",
"PCK345" or "SPIRAL" and <no_pixels> gives the total number of pixels
in the image.  Example: FORMAT 1200 SPIRAL 1111647

HIGH <n_high>
Number of pixels with values > 65535 (16-bit). Depending on this
value, programs should try to read <n_high> high intensity pixel pairs
(address in array and 32-bit pixel value) preceding the 16-bit data
array.  Example: HIGH 0

PIXEL LENGTH <pix_length> HEIGHT <pix_height>
Size of one pixel in micron units (1000. * mm). In SPIRAL format,
length is shorter than height (double sampling).  Example: PIXEL LENGTH 75 HEIGHT 150

OFFSET ROFF <roff> TOFF <toff>
Radial and tangential offset of the scanner in mm (constants). 
Example: OFFSET ROFF 0.1 TOFF -0.05

MULTIPLIER <fmul>
High intensity multiplier. 
Example: MULTIPLIER 1.000

GAIN <gain>
Gain of the detector, i.e. the number of photons required to produce 1 ADC unit. 
Example: GAIN 1.000

WAVELENGTH <wave>
Used wavelength in Angstroems. 
Example: WAVELENGTH 0.7107

DISTANCE <distance>
Used distance crystal to detector in mm. 
Example: DISTANCE 70.0

RESOLUTION <dmax>
Maximum resolution (Ang.) at edge of the plate. 
Example: RESOLUTION 2.1

PHI START <phi_start> END <phi_end> OSC <n_osc>
PHI values at start and end of exposure. <n_osc> is the number of PHI oscillations during this exposure. 
Example: PHI START 10.000 END 11.000 OSC 1

OMEGA START <omega_start> END <omega_end> OSC <n_osc>
OMEGA values at start and end of exposure. <n_osc> is the number of
omega oscillations during this exposure.
Example: OMEGA START 0.000 END 0.000 OSC 0

CHI <chi>
CHI value during this exposure. 
Example: CHI 90.0

TWOTHETA <twotheta>
2-theta value during this exposure. 
Example: TWOTHETA 0.0

CENTER X <xcen> Y <ycen>
Coordinates of direct beam in pixel units. 
Example: CENTER X 999.000 Y 1001.100

MODE <dcmode>
Mode of data collection: "TIME" or "DOSE". 
Example: MODE TIME

TIME <exp_time>
Exposure time in seconds. In DOSE mode, the time varies depending to X-ray flux. 
Example: TIME 60.00

COUNTS START <cnt_beg> END <cnt_end> MIN <cnt_min> MAX <cnt_max> AVE <cnt_ave> SIG <cnt_sig> NMEAS <cnt_n>

X-ray counts as measured by the second ionization chamber: minimum,
maximum and average value, values at start and end of exposure and
sigma of average reading. <cnt_n> is the number of times the X-ray
intensity has actually been read during the exposure.
Example: COUNTS START 12.1 END 11.50 MIN 10.9 MAX 12.4 AVE 11.6 SIG 0.8 NMEAS 120

INTENSITY MIN <int_min> MAX <int_max> AVE <int_ave> SIG <int_sig>

Pixel values in image: minimum, maximum, average value and sigma. 
Example: INTENSITY MIN 8 MAX 32987 AVE 432.4 SIG 25.9

HISTOGRAM START <his_beg> END <his_end> MAX <his_max>
Distribution of pixel values in image (used for distributing colours
at display). <his_beg> and <his_end> give the limits for the range of
intensities to distribute available colours. <his_max> is the most
frequent pixel value in the image.
Example: HISTOGRAM START 120 END 640 MAX 216

GENERATOR <type> kV <kiloVolt> mA <milliAmps>
Type of x-ray source ("SEALED TUBE", "ROTATING ANODE" or
"SYNCHROTRON") and power settings.

Example: GENERATOR SEALED TUBE kV 40.0 mA 50.0

MONOCHROMATOR <type> POLAR <polar>

Type of monochromator ("GRAPHITE", "MIRRORS" or "FILTER") and value of x-ray polarization. 
Example: MONOCHROMATOR GRAPHITE POLAR 0.000

COLLIMATOR WIDTH <width> HEIGHT <height>
Aperture of horizontal (<width>) and vertical (<height>) slits in
mm. These values are for the second of the two slit systems of the mar
research collimation system. They determine the size of the beam.

Example: COLLIMATOR WIDTH 0.3 HEIGHT 0.3

REMARK <text>
Additional remark (one line only) 
Example: REMARK Lysozyme crystal 0.5x0.8 mm

*/

static int swapFlag = 0;

void swapInt( unsigned int *p) {
  unsigned char c0, c1, c2, c3, *cp;

  cp = (unsigned char *)p;
  c0 = *cp++;
  c1 = *cp++;
  c2 = *cp++;
  c3 = *cp;
  *cp-- = c0;
  *cp-- = c1;
  *cp-- = c2;
  *cp   = c3;
}


//
// add key/value pair to header information
// New value with same key replaces old pair
//
void mar345_add_header_info( char *k, char *v) {
  mar345_ascii_header_type *t;

  //
  // Unlike adsc headers, mar345 headers include all
  // values with the same key, not just the last one
  //
  t = calloc( 1, sizeof( mar345_ascii_header_type));
  if( t==NULL) {
    fprintf( stderr, "out of memory (k=%s, v=%s)", k, v);
    exit( 1);
  }

  t->next = theMar345AsciiHeader;
  t->key = k;
  t->val = v;
  theMar345AsciiHeader = t;
}


//
// return a string value of a key
//
char *mar345_header_get_str( char *k) {
  char *rtn;
  mar345_ascii_header_type *h;

  rtn = "";
  for( h = theMar345AsciiHeader; h != NULL; h = h->next) {
    if( strcasecmp( k, h->key)==0) {
      rtn = h->val;
      break;
    }  
  }
  return( rtn);
}

//
// return an integre value of a key
//
int mar345_header_get_int( char *k) {
  int rtn;

  rtn = atol( mar345_header_get_str( k));
  return( rtn);
}

//
// parse the header into key/value pairs
// Make everything uppercase to simplify comparisons
//
void mar345_header_parse( char *h) {
  unsigned int *tp, *mbhp;
  int i;

  char *k, *v, *hp;
  int state;  // 0 wait to start, 1 read key, 2 read val, 3 done
  
  //
  // First get binary header:
  //
  tp = (unsigned int *)h;
  //
  // Check byte swap flag
  // OK to assume not 1234 means swap needed as value was proven OK beforehand
  //
  if( *tp != 1234) {
    swapFlag = 1;
    for( i=0; i<16; i++, tp++) {
      swapInt( tp);
    }
  }
  mbhp = (unsigned int *)&theMar345BinHeader;
  tp   = (unsigned int *)h;
  for( i=0; i<16; i++, tp++, mbhp++) {
    *mbhp = *tp;
  }
  


  //
  // Now get the ascii header
  //

  h[4095] = 0;	// prevents falling off into never-never land

  state = 0;	// initial state engine state

  hp = h + 128;	// Skip Binnary header

  while( *hp != 0) {
    switch( state) {
      //
      // Looking for start of header
      //
    case 0:
      while( *hp != '\n' && *hp != 0) hp++;
      if( *hp == '\n') {
	hp++;
	state = 1;
      }
      break;

      //
      // Looking for Key
      //
    case 1:
      //
      // Bypass white space
      //
      while( isspace( *hp)) hp++;

      //
      // Start of Key
      //
      k = hp;
      while( *hp != 0 && !isspace( *hp)) { *hp=toupper(*hp);hp++;}

      //
      // Mark end of string and look for value 
      // or cleanup and leave if end of header found
      //
      if( isspace( *hp)) {
	*hp = 0;
	hp++;
	state = 2;	// look for value
      } else {
	state = 3;	// cleanup and leave
      }
      break;

      //
      // Looking for Value
      //
    case 2:
      //
      // Ignore white space
      //
      while( isspace( *hp)) hp++;
      //
      // Start of value
      //
      v = hp;
      while( *hp != 0 && *hp != '\n') { *hp=toupper(*hp);hp++;}

      //
      // Mark end of string and add key/value pair
      // End the maddness if null or closing brace is found
      //
      //      printf( "'%s' %d  '%s' %d\n", k, strcmp( k, "END"), v, strcmp( v, "OF HEADER"));
      if( strncmp( k, "END", 3)==0 && strncmp( v, "OF HEADER", 9)==0) {
	state = 3;
      } else if( *hp == 0) {
	mar345_add_header_info( k, v);
	state = 3;			// clean up and leave
      } else {
	*hp = 0;
	mar345_add_header_info( k, v);
	hp++;
	state = 1;			// look for new key
      }
      break;

    case 3:
      *hp = 0;
      break;
    }
  }
}

void mar345_header_print() {
  mar345_ascii_header_type *h;
  mar345_bin_header_type *bhp;
  bhp = &theMar345BinHeader;

  fprintf( stderr, "            Swap Flag:  %d\n", bhp->swapFlag);
  fprintf( stderr, "  rows/cols of pixels:  %d\n", bhp->n1Size);
  fprintf( stderr, "high intensity pixels:  %d\n", bhp->nHi);
  fprintf( stderr, "         Image Format:  %s\n", (bhp->imageFormat==1 ? "Compressed" : (bhp->imageFormat==2 ? "Spiral" : "Unknown")));
  fprintf( stderr, "      Collection Mode:  %s\n", (bhp->collectionMode==0 ? "Dose" : (bhp->collectionMode==1 ? "Time" : "Unknown")));
  fprintf( stderr, "         Total Pixels:  %d\n",                     bhp->nPixels);
  fprintf( stderr, "         Pixel Length:  %d microns\n",             bhp->pixelLength);
  fprintf( stderr, "         Pixel Height:  %d microns\n",             bhp->pixelHeight);
  fprintf( stderr, "           Wavelength:  %0.6f Angstorm\n", (double)bhp->wavelength/1.0e6);
  fprintf( stderr, "             Distance:  %0.3f mm\n",       (double)bhp->distance/1.0e3);
  fprintf( stderr, "            Start Phi:  %0.3f deg\n",      (double)bhp->phiStart/1.0e3);
  fprintf( stderr, "              End Phi:  %0.3f deg\n",      (double)bhp->phiEnd/1.0e3);
  fprintf( stderr, "          Start Omega:  %0.3f deg\n",      (double)bhp->omegaStart/1.0e3);
  fprintf( stderr, "            End Omega:  %0.3f deg\n",      (double)bhp->omegaEnd/1.0e3);
  fprintf( stderr, "                  Chi:  %0.3f deg\n",      (double)bhp->chi/1.0e3);
  fprintf( stderr, "            Two Theta:  %0.3f deg\n",      (double)bhp->twotheta/1.0e3);

  for( h = theMar345AsciiHeader; h!=NULL; h = h->next) {
    fprintf( stderr, "key: '%s'    val: '%s'\n", h->key, h->val);
  }

}

FILE *mar345_fopen( char *fn, char *mode) {
  static char header[4096];
  FILE *rtn;
  int err;

  rtn = fopen( fn, mode);
  if( rtn == NULL) {
    return( NULL);
  }

  err = fread( header, 1, 4096, rtn);
  if( err != 4096) {
    return( NULL);
  }

  mar345_header_parse( header);

  return( rtn);
}


void mar3452jpeg() {
  FILE *f;
  //
  // sign bit masks
  //
  int sbms[] = {0x00000001, 0x00000002, 0x00000004, 0x00000008, 0x00000010, 0x00000020, 0x00000040, 0x00000080,
		0x00000100, 0x00000200, 0x00000400, 0x00000800, 0x00001000, 0x00002000, 0x00004000, 0x00008000,
		0x00010000, 0x00020000, 0x00040000, 0x00080000, 0x00100000, 0x00200000, 0x00400000, 0x00800000,
		0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000
  };
  int sbm;
  //
  // sign extend masks
  //
  int sems[] = { 0xffffffff, 0xfffffffe, 0xfffffffc, 0xfffffff8, 0xfffffff0, 0xffffffe0, 0xffffffc0, 0xffffff80,
		 0xffffff00, 0xfffffe00, 0xfffffc00, 0xfffff800, 0xfffff000, 0xffffe000, 0xffffc000, 0xffff8000,
		 0xffff0000, 0xfffe0000, 0xfffc0000, 0xfff80000, 0xfff00000, 0xffe00000, 0xffc00000, 0xff800000,
		 0xff000000, 0xfe000000, 0xfc000000, 0xf8000000, 0xf0000000, 0xe0000000, 0xc0000000, 0x80000000
  };
  int sem;

  mar345_overflow_type *ovfl;
  

  unsigned int npix, npixRead;
  int nbits, init, in, incount, next;

  unsigned short *buf, *bp;
  unsigned char  c;
  unsigned int length, width;
  unsigned int rslt;
  unsigned int  *ip;
  int i;
  
  // kludge just to compile, need to go over code
  int x, org_data[245], need, pixcount;

  f = mar345_fopen( filename, "r");

  if( debug) {
    mar345_header_print();
  }

  //
  // get the overflows
  //
  ovfl = calloc( theMar345BinHeader.nHi, sizeof( mar345_overflow_type));
  if( ovfl == NULL) {
    fprintf( stderr, "Out of memory for %d overflow pixels\n", theMar345BinHeader.nHi);
    exit( 1);
  }
  
  rslt = fread( ovfl, sizeof( mar345_overflow_type), theMar345BinHeader.nHi, f);
  if( rslt != theMar345BinHeader.nHi) {
    fprintf( stderr, "Read %d overflows out of %d\n", rslt, theMar345BinHeader.nHi);
    exit( 1);
  }
  //
  // swap em?
  //
  if( swapFlag == 1) {
    for( i=0; i<theMar345BinHeader.nHi; i++) {
      ip = (unsigned int *) &ovfl[i].location;
      swapInt( ip);
      ip = (unsigned int *) &ovfl[i].value;
      swapInt( ip);
    }
  }

  //
  // get to the data
  //
  fseek( f, ((int)((float)theMar345BinHeader.nHi/8.0 + 0.875)) * 64 + 4096, SEEK_SET);

  //
  // skip over "CCP4 packed image, X: 3450, Y: 3450\n"
  // If someone adds more junk before the data then this section will need to get smartter
  //
  c = 0;
  while( c != '\n') {
    rslt = fgetc( f);
    if( rslt == -1) {
      fprintf( stderr, "Error reading Mar345 file\n");
      exit( 1);
    }
    c = rslt;
  }

  npix     = theMar345BinHeader.nPixels;
  npixRead = 0;
  width    = theMar345BinHeader.n1Size;
  length   = theMar345BinHeader.n1Size;

  buf = calloc( npix, sizeof( unsigned short));
  if( buf == NULL) {
    fprintf( stderr, "Out of memory allocating %d pixels for Mar345 image\n", npix);
    exit( 1);
  }

  //
  //  Bitstream of data encoded as follows:
  //
  //  first 6 bits:
  //   dddnnn
  //     ddd: number of bits to get for this run
  //          encoded as 2^(ddd) (1 to 128)
  //     nnn: bits to use from the stream for each pixel
  //          encoded as index to array: [0,4,5,6,7,8,16,32]
  //
  //   The value decoded from the bitstream is added to the
  //   average value of nearby pixels previously read.
  //

  in = incount = 0;
  nbits = 6;
  init = 1;
  x = org_data [0];
  bp = buf;

  while( npixRead < npix) {
      /* Get the next "nbits" bits of data into "next" */
    next = 0;
    need = nbits;
    while( need) {
      if( incount == 0) {
	rslt = fgetc (f);
        if (rslt < 0) {
	  fprintf( stderr, "End of file after %d pixels out of %d read", npixRead, npix);
	  exit( 1);
        }
	c = rslt;
        incount = 8;
      }

      if( need > incount) {
	//
	// push the bits into next
        next |= c << (nbits - need);
        need -= incount;
        c = 0;
        incount = 0;
      }  else {
	//
	//       mask off need bits and move the mask over
        next |= (c & ((1 << need) - 1)) << (nbits - need);
	//
	// get rid of bits we just used
        c = (c >> need) & 0xff;
        incount -= need;
        break;
      }
    }

      /* Decode bits 0-5 */
    if( init) {
      // upper 3 bits determines the number of bits to get next time
      static int decode[8] = { 0, 4, 5, 6, 7, 8, 16, 32 };
      //
      // lower 3 bits is n of 2^n pixels in this set
      //
      pixcount = 1 << (next & 7);
      //
      // find the number of bits next round
      //
      nbits = decode[(next >> 3) & 7];
      sem = sems[nbits];
      sbm = sbms[nbits];
      init = 0;

    } else {
        /* Decode a pixel */
        /* Sign-extend? */
      if( nbits && (sbm & next))
	next |= sem;

        /* Calculate the final pixel value */
      if( npixRead > x) {
        int A, B, C, D;

	A = *(bp - x - 1);	// last row, last column
	B = *(bp - x);		// last row, same column
	C = *(bp - x + 1);	// last row, next column
	D = *(bp - 1);		// same row, last column

        *bp = (next + (((A & 0x7fff) + 
			(B & 0x7fff) + 
			(C & 0x7fff) +
			(D & 0x7fff) -
			(A & 0x8000) -
			(B & 0x8000) -
			(C & 0x8000) -
			(D & 0x8000) + 2) / 4)) & 0xffff;
      } else
        if( npixRead)
	  *bp  = ( *(bp-1) + next) & 0xffff;	// next is increment from previous
        else
	  *bp = next & 0xffff;			// ya got ta start somewhere

      npixRead++;
      bp++;
      pixcount--;
      /* New set? */
      if (pixcount == 0) {
        init = 1;
        nbits = 6;
      }
    }
  }

  return;
}

