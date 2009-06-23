#include "is.h"

void marTiff2jpeg( ) {
  TIFF *tf;
  struct jpeg_compress_struct cinfo;
  struct jpeg_error_mgr jerr;
  unsigned short *buf;
  unsigned char  *bufo;
  unsigned int length, width;
  unsigned short d, d1;
  unsigned char dout;
  unsigned int rslt;
  tsize_t sls;
  int i, j, k, l, m, n, ya, xa;
  int newL, newT, newW, newH;
  FILE *jout;
  JSAMPROW jsp[1];

  tf = TIFFOpen( filename, "r");

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

  if( tf != NULL) {
    TIFFGetField( tf, TIFFTAG_IMAGELENGTH,   &length);
    TIFFGetField( tf, TIFFTAG_IMAGEWIDTH,    &width);

    sls = TIFFScanlineSize( tf);
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
      TIFFReadScanline( tf, buf + i*width, i, 0);
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
	// default pixel has maximum value
	//
	d = 65535;

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
