#include "is.h"

void write_row_callback( png_structp png_ptr, png_uint_32 row, int pass) {
  return;
}

void marTiff2png( ) {
  TIFF *tf;

  unsigned short *buf;
  unsigned char  *bufo;
  unsigned int length, width;
  unsigned short d, d1;
  unsigned char dout;
  unsigned int rslt;
  tsize_t sls;
  int i, j, k, l, m, n, ya, xa;
  int newL, newT, newW, newH;

  png_structp png_ptr;
  png_infop info_ptr;
  png_bytep row_pointer;

  tf = TIFFOpen( filename, "r");


  png_ptr = png_create_write_struct( PNG_LIBPNG_VER_STRING, (png_voidp)NULL, NULL, NULL);
  if( !png_ptr)
    return;

  info_ptr = png_create_info_struct( png_ptr);
  if( !info_ptr) {
    png_destroy_write_struct( &png_ptr, (png_infopp)NULL);
    return;
  }

  if( setjmp(png_jmpbuf(png_ptr))) {
    png_destroy_write_struct(&png_ptr, &info_ptr);
    return;
  }

  png_init_io( png_ptr, stdout);
  png_set_write_status_fn( png_ptr, write_row_callback);

  png_set_IHDR( png_ptr, info_ptr, xsize, ysize, 8, PNG_COLOR_TYPE_RGB_ALPHA, PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);
  //  png_set_IHDR( png_ptr, info_ptr, xsize, ysize, 8, PNG_COLOR_TYPE_RGB, PNG_INTERLACE_NONE, PNG_COMPRESSION_TYPE_DEFAULT, PNG_FILTER_TYPE_DEFAULT);

  png_write_info( png_ptr, info_ptr);

  if( tf != NULL) {
    TIFFGetField( tf, TIFFTAG_IMAGELENGTH,   &length);
    TIFFGetField( tf, TIFFTAG_IMAGEWIDTH,    &width);

    sls = TIFFScanlineSize( tf);
    buf  = malloc( sls * length);

    //    bufo = calloc( 3 * ysize * xsize, sizeof(unsigned char) );
    bufo = calloc( 4 * ysize * xsize, sizeof(unsigned char) );

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

	// 16 bit encoding

	*(bufo + 4*i*xsize + 4*j    )  = ((d & 0xff00) >> 8) & 0xff;
	*(bufo + 4*i*xsize + 4*j + 1)  = (d & 0x00ff) & 0xff;
	*(bufo + 4*i*xsize + 4*j + 2)  = 255 - dout;
	*(bufo + 4*i*xsize + 4*j + 3)  = (d==65535) ? 0 : 255;

	/*
	if( d==65535) {
	  *(bufo + 3*i*xsize + 3*j    ) = 255;	// prefer red for saturation
	  *(bufo + 3*i*xsize + 3*j + 1) =   0;
	  *(bufo + 3*i*xsize + 3*j + 2) =   0;
	} else {
	  *(bufo + 3*i*xsize + 3*j)     = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 1) = 255 - dout;
	  *(bufo + 3*i*xsize + 3*j + 2) = 255 - dout;
	}
	*/
      }

      row_pointer = bufo + 4*i*xsize;
      //      row_pointer = bufo + 3*i*xsize;
      png_write_row( png_ptr, row_pointer);
    }
    TIFFClose( tf);

    png_write_end( png_ptr, info_ptr);
    fclose( stdout);
    png_destroy_write_struct( &png_ptr, &info_ptr);
  }
}
