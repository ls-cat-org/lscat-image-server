#include "is.h"

void marTiff2jpeg( ) {
  TIFF *tf;
  struct jpeg_compress_struct cinfo;
  struct jpeg_error_mgr jerr;
  unsigned short *buf;
  unsigned char  *bufo;
  unsigned short d, d1;
  unsigned char dout;
  unsigned int rslt;
  unsigned int iheight, iwidth;
  tsize_t sls;
  int i, j, k, l, m, n;
  float ya, xa;
  FILE *jout;
  JSAMPROW jsp[1];

  tf = TIFFOpen( filename, "r");

  cinfo.err = jpeg_std_error(&jerr);
  jpeg_create_compress(&cinfo);

  if( fout == NULL)
    jout = stdout;
  else
    jout = fout;

  jpeg_stdio_dest(&cinfo, jout);
  cinfo.image_width = xsize;		/* image width and height, in pixels */
  cinfo.image_height = ysize;

  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */

  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 100, TRUE);

  jpeg_start_compress(&cinfo, TRUE);

  if( tf != NULL) {
    TIFFGetField( tf, TIFFTAG_IMAGELENGTH,   &iheight);
    TIFFGetField( tf, TIFFTAG_IMAGEWIDTH,    &iwidth);

    //    fprintf( stderr, "iheight: %d  iwidth: %d\n", iheight, iwidth);
    //    fprintf( stderr, "height: %d  width: %d\n", height, width);
    //    fprintf( stderr, "xstart: %d  ystart: %d\n", xstart, ystart);
    //    fprintf( stderr, "xsize: %d  ysize: %d\n", xsize, ysize);

    sls = TIFFScanlineSize( tf);
    buf  = malloc( sls * height);

    bufo = calloc( 3 * ysize * xsize, sizeof(unsigned char) );

    //
    // size of rectangle to search for the maximum pixel value
    //
    ya = (double)height/(double)ysize;
    xa = (double)width/(double)xsize;

    //    fprintf( stderr, "ya: %f  xa: %f\n", ya, xa);


    //
    // read only the rows only that will appear in the final image
    //
    //    fprintf( stderr, "ystart-1: %d   ystart+height+1: %d\n", ystart-1, ystart+height+1);
    for( i = (int)ystart - (int)1; i<(int)(ystart+height+1); i++) {
      if( i<0 || i>=iheight)
	continue;
      TIFFReadScanline( tf, buf + i*iwidth, i, 0);
    }

    //
    // loop over pixels in the new image
    //
    for( i=0; i<ysize; i++) {
      //
      // map pixel horz index in old image
      k = i * height;
      k /= ysize;
      k += ystart;

      for( j=0; j<xsize; j++) {
	l = j * width;
	l /= xsize;
	l += xstart;

	//
	// default pixel has maximum value, er, zero
	//
	d = 0;

	if( l>=0 && k>=0 && l<iwidth && k<iheight) {
	  //
	  //  If we are on the orginal image, get the pixel value
	  //
	  if( k>=0 && k<iheight) { 
	    if( l>=0 && l<iwidth)
	      d = *(buf + k*iwidth+l);
	  }
	  //	  fprintf( stderr, "l: %d  k: %d  d: %d\n", l, k, d);


	  //
	  // Look around for the maximum value when the ouput image is
	  // being reduced
	  //
	  if( ya>0. || xa>0.) {
	    for( m=floor(k-ya/2.)*iwidth; m<ceil(k+ya/2.)*iwidth; m+=iwidth) {
	      if( m<0 || m>=iheight*iwidth)
		continue;
	      for( n=floor(l-xa/2.); n<ceil(l+xa/2.); n++) {
		if( n<0 || n>= iwidth)
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
	  *(bufo + 3*i*xsize + 3*j + 1) = 0;
	  *(bufo + 3*i*xsize + 3*j + 2) = 0;
	} else {
	  *(bufo + 3*i*xsize + 3*j)     = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 1) = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 2) = 255 - dout;
	}
      }

      jsp[0] = bufo + 3*i*xsize;
      jpeg_write_scanlines(&cinfo, jsp, 1);
    }
    TIFFClose( tf);

    jpeg_finish_compress(&cinfo);
    fclose( jout);
  }
}
