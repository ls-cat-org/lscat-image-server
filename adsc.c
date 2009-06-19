#include "is.h"

//
// add key/value pair to header information
// New value with same key replaces old pair
//
void adsc_add_header_info( char *k, char *v) {
  adsc_header_type *t, *tl;

  for( tl=t=theHeader; t != NULL; t = t->next) {
    if( strcmp( k, t->key)==0) {
      t->val = v;
      break;
    }
    tl = t;
  }
  if( t == NULL) {
    t = calloc( 1, sizeof( adsc_header_type));
    if( t==NULL) {
      fprintf( stderr, "out of memory (k=%s, v=%s)", k, v);
      exit( 1);
    }
    t->next = NULL;
    t->key = k;
    t->val = v;
    if( theHeader == NULL) {
      theHeader = t;
    } else {
      tl->next = t;
    }
  }
}


//
// return a string value of a key
//
char *adsc_header_get_str( char *k) {
  char *rtn;
  adsc_header_type *h;

  rtn = "";
  for( h = theHeader; h != NULL; h = h->next) {
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
int adsc_header_get_int( char *k) {
  int rtn;

  rtn = atol( adsc_header_get_str( k));
  return( rtn);
}

//
// parse the header into key/value pairs
// Make everything uppercase to simplify comparisons
//
void adsc_header_parse( char *h) {
  char *k, *v, *hp;
  int state;  // 0 wait to start, 1 read key, 2 read val, 3 done

  h[511] = 0;
  state = 0;
  hp = h;
  while( *hp != 0) {
    switch( state) {
      //
      // Looking for start of header
      //
    case 0:
      while( *hp != '{' && *hp != 0) hp++;
      if( *hp == '{') {
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
      while( *hp != '=' && *hp != 0 && !isspace( *hp) && *hp != '}') { *hp=toupper(*hp);hp++;}

      //
      // Mark end of string and look for value 
      // or cleanup and leave if end of header found
      //
      if( *hp == '=' || isspace( *hp)) {
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
      while( isspace( *hp) || *hp == '=') hp++;
      //
      // Start of value
      //
      v = hp;
      while( *hp != 0 && !isspace( *hp) && *hp != '}' && *hp != ';') { *hp=toupper(*hp);hp++;}

      //
      // Mark end of string and add key/value pair
      // End the maddness if null or closing brace is found
      //
      if( *hp == 0 || *hp == '}') {
	adsc_add_header_info( k, v);
	state = 3;			// clean up and leave
      } else {
	*hp = 0;
	adsc_add_header_info( k, v);
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

void adsc_header_print() {
  adsc_header_type *h;

  for( h = theHeader; h!=NULL; h = h->next) {
    fprintf( stderr, "key: '%s'    val: '%s'\n", h->key, h->val);
  }

}

FILE *adsc_fopen( char *fn, char *mode) {
  static char header[512];
  FILE *rtn;
  int err;

  rtn = fopen( fn, mode);
  if( rtn == NULL) {
    return( NULL);
  }

  err = fread( header, 1, 512, rtn);
  if( err != 512) {
    return( NULL);
  }

  adsc_header_parse( header);

  return( rtn);
}


void adsc2jpeg() {
  FILE *f;
  struct jpeg_compress_struct cinfo;
  struct jpeg_error_mgr jerr;

  unsigned short *buf;
  unsigned char  *bufo;
  unsigned int length, width;
  unsigned short d, d1;
  unsigned char dout;
  unsigned int rslt;
  long offset;
  unsigned int  sls;
  int i, j, k, l, m, n, ya, xa;
  int newL, newT, newW, newH;
  FILE *jout;
  JSAMPROW jsp[1];

  int bs, bsj;
  unsigned short *bsBuf;
  unsigned short c1, c2;

  unsigned short endianS;	// used to see what kind of endian we have
  unsigned char  *endianCp;	// used to see what kind of endian we have
  int weAreBigEndian;		// flag if we are big

  
  if( debug) {
    fprintf( stderr, "adsc2jpeg: opening file %s\n", filename);
  }
  f = adsc_fopen( filename, "r");

  if( debug) {
    adsc_header_print();
  }

  cinfo.err = jpeg_std_error(&jerr);
  jpeg_create_compress(&cinfo);

  jout = stdout;

  jpeg_stdio_dest(&cinfo, jout);
  cinfo.image_width = xsize;		/* image width and height, in pixels */
  cinfo.image_height = ysize;

  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */

  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, jpq, TRUE);

  jpeg_start_compress(&cinfo, TRUE);

  if( f != NULL) {

    //
    // We will need to know if we have to do byte swapping of the data
    // For this we need to see what endian machine we are running on
    //
    endianS = 0x1234;
    endianCp = (unsigned char *)&endianS;
    if( *endianCp == 0x34) {
      weAreBigEndian = 0;
    } else if( *endianCp == 0x12) {
      weAreBigEndian = 1;
    } else {
      weAreBigEndian = 1;
      fprintf( stderr, "Running on machine of unknown endian type!\n");
    }


    //
    // set the byte swap flag if data and our processor are not the same kind of endian
    //
    bs = (strstr( adsc_header_get_str( "BYTE_ORDER"), "BIG") != NULL) ? 1 - weAreBigEndian : weAreBigEndian;


    width  = adsc_header_get_int( "SIZE1");
    length = adsc_header_get_int( "SIZE2");

    if( debug) {
      fprintf( stderr, "width: %d   length: %d\n", width, length);
    }

    sls = 2 * length;

    buf  = malloc( sls * length);

    bufo = calloc( 3 * ysize * xsize, sizeof(unsigned char) );

    //
    //  Define the portion of the original image that is to appear in
    //  the output.
    //
    newW = width/zoom;
    newH = length/zoom;
    newL = (width  - newW)/2.0 + width*(xcen - 0.5);
    if( newL < 0)
      newL++;
    newT = (length - newH)/2.0 + length*(ycen - 0.5);
    if( newT < 0)
      newT++;

    //
    // size of rectangle to search for the maximum pixel value
    //
    ya = newH/ysize;
    xa = newW/xsize;

    //
    // read only the rows only that will appear in the final image
    //
    for( i=newT-1; i<newT+newH+1; i++) {
      if( i<0 || i>=length)
	continue;
      offset = 512 + i*width*2;
      fseek( f, offset, SEEK_SET);
      fread( buf + i*width, 2, width, f);
      if( bs) {
	bsBuf = buf + i*width;
	for( bsj=0; bsj<width; bsj++, bsBuf++) {
	  c1 = (*bsBuf & 0x00ff) << 8;
	  c2 = (0xff00 & *bsBuf) >> 8;
	  *bsBuf = c1  | c2;
	}
      }
    }

    //
    // loop over pixels in the new image
    //
    for( i=0; i<ysize; i++) {
      //
      // map pixel horz index in old image
      k = i * newH;
      k /= ysize;
      k += newT;

      for( j=0; j<xsize; j++) {
	l = j * newW;
	l /= xsize;
	l += newL;

	//
	// default pixel has no value  (white)
	//
	d = 0;

	if( l>=0 && k>=0 && l<width && k<length) {
	  //
	  //  If we are on the orginal image, get the pixel value
	  //
	  if( k>=0 && k<length) { 
	    if( l>=0 && l<width)
	      d = *(buf + k*width+l);
	  }

	  //
	  // Look around for the maximum value when the ouput image is
	  // being reduced
	  //
	  if( ya>0 || xa>0) {
	    for( m=(k-ya/2)*width; m<(k+ya/2)*width; m+=width) {
	      if( m<0 || m>=length*width)
		continue;
	      for( n=l-xa/2; n<l+xa/2; n++) {
		if( n<0 || n>= width)
		  continue;
		d1 = *(buf + m + n);
		d = (d>d1 ? d : d1);
	      }
	    }
	  }
	}

	if( d <= wpixel) {
	  dout = 0;
	} else {
	  if( d >= bpixel) {
	    dout = 255;
	  } else {
	    rslt = (d - wpixel) * 255;
	    dout = rslt/(bpixel-wpixel);
	  }
	}

	if( d==65535) {
	  *(bufo + 3*i*xsize + 3*j    ) = 255;
	  *(bufo + 3*i*xsize + 3*j + 1) = 255;
	  *(bufo + 3*i*xsize + 3*j + 2) =  51;
	} else {
	  *(bufo + 3*i*xsize + 3*j)     = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 1) = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 2) = 255 - dout;
	}
      }

      jsp[0] = bufo + 3*i*xsize;
      jpeg_write_scanlines(&cinfo, jsp, 1);
    }
    fclose( f);

    jpeg_finish_compress(&cinfo);
    fclose( jout);
  }
}
