#include "is.h"

extern void marTiff2jpeg( isType *is);
extern void marTiff2profile( isType *is);

void marTiff( isType *is) {
  if( strcmp( is->cmd, "jpeg") == 0) {
    marTiff2jpeg( is);
  } else {
    if( strcmp( is->cmd, "profile") == 0)
    marTiff2profile( is);
  }
}

unsigned short nearestValue( isType *is, double k, double l) {
  return *(is->buf + (int)(k+0.5)*(is->inWidth)+(int)(l+0.5));
}


/*
** returns the maximum value of ha xa by ya box centered on d,l
*/
unsigned short maxBox( isType *is, unsigned short *buf, double k, double l, int yal, int yau, int xal, int xau) {
  int m, n;
  unsigned short d, d1;

  d = 0;

  for( m=k-yal; m < k+yau; m++) {
    for( n=l-xal; n<l+xau; n++) {
      d1 = *(buf + m*(is->inWidth) + n);
      d = (d>d1 ? d : d1);
    }
  }

  return d;
}

void marTiffRead( isType *is) {
  //
  // is is the image structure we are getting all our info from
  // pad is the amount of extra room to leave on the RHS
  // in addtion to extra scans line at the top and bottom
  //
  TIFF *tf;
  int sls;
  int i;

  //  TIFFSetErrorHandler( NULL);		// surpress annoying error messages 
  //  TIFFSetWarningHandler( NULL);		// surpress annoying warning messages 
  tf = TIFFOpen( is->fn, "r");		// open the file
  if( tf == NULL) {
    fprintf( stderr, "marTiffRead failed to open file '%s'\n", is->fn);
    return;
  }
  TIFFGetField( tf, TIFFTAG_IMAGELENGTH,   &(is->inHeight));
  TIFFGetField( tf, TIFFTAG_IMAGEWIDTH,    &(is->inWidth));

  // add the padding here
  sls = sizeof(unsigned short) * (is->inWidth + is->pad);

  // use calloc to be sure unassigned pixels have zero value
  //
  is->fullbuf  = calloc( sls * (is->inHeight + 2*is->pad), sizeof( unsigned short));
  if( is->fullbuf == NULL) {
    TIFFClose( tf);
    fprintf( stderr, "marTiffRead: Out of memory.  malloc(%d) failed\n", sls * (is->inHeight+1));
    return;
  }

  // let the first scan line be blank
  is->buf = is->fullbuf + sls*is->pad;
    
  //
  // read the image
  //

  for( i=0; i<is->inHeight; i++) {
    TIFFReadScanline( tf, is->buf + i*(is->inWidth), i, 0);
  }
  
  //
  // we are done
  //
  TIFFClose( tf);
  
  return;
}


void jerror_handler( j_common_ptr cp) {
  fprintf( stderr, "Is that socket still there?  I think not!\n");  
  longjmp( *(jmp_buf *)cp->client_data, 1);
}


void marTiff2jpeg( isType *is ) {
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  unsigned short *buf;
  unsigned char  *bufo;
  unsigned char  *bp;
  unsigned short d;
  unsigned char dout;
  unsigned int rslt;
  int i, j;
  int jmin, jmax;
  double k, l;
  int ya, xa;
  int yal, yau, xal, xau;
  int tyal, tyau;
  JSAMPROW jsp[1];


  cinfoSetup = 0;
  buf = NULL;
  bufo = NULL;

  if( setjmp( j_jumpHere)) {
    if( cinfoSetup)
      jpeg_destroy_compress( &cinfo);
    if( buf != NULL)
      free( is->fullbuf);
    if( bufo != NULL)
      free( bufo);

    return;
  }
  

  //
  // size of rectangle to search for the maximum pixel value
  // yal and xal are subtracted from ya and xa for the lower bound of the box and
  // yau and xau are added to ya and xa for the upper bound of the box
  //
  ya = (is->height)/(is->ysize);
  xa = (is->width)/(is->xsize);
  yal = yau = ya/2;
  if( (yal + yau) < ya)
    yau++;

  xal = xau = xa/2;
  if( (xal+xau) < xa)
    xau++;

  //
  // get the data, pad scan line by xal
  is->pad = xal;
  marTiffRead( is);
  buf = is->buf;

  cinfo.err = jpeg_std_error(&jerr);
  cinfo.err->error_exit = jerror_handler;
  cinfo.client_data    = &j_jumpHere;
  jpeg_create_compress(&cinfo);
  cinfoSetup = 1;

  jpeg_stdio_dest(&cinfo, is->fout);
  cinfo.image_width = is->xsize;		/* image width and height, in pixels */
  cinfo.image_height = is->ysize;

  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */

  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 100, TRUE);

  jpeg_start_compress(&cinfo, TRUE);

  bufo = calloc( 3 * is->ysize * is->xsize, sizeof(unsigned char) );
  if( bufo == NULL) {
    fprintf( stderr, "marTiff2jpeg: Out of memory.  calloc(%d) failed\n", 3*is->ysize*is->xsize*sizeof( unsigned char));
    return;
  }

  //
  // compute the range of j, ignoring for now the j's that, considering the box, would lead to pixels off the input image
  //

  jmin = -(is->x)*(is->xsize)/(is->width);
  jmax = ((is->inWidth)-(is->x)) * (is->xsize)/(is->width);
  if( jmin < 0)
    jmin = 0;
  if( jmax > is->xsize)
    jmax = is->xsize;

  //
  // loop over pixels in the output image
  //
  // i index over output image height (is->ysize)
  // j index over output image width  (is->xsize)
  //
  // double k maps i into the input image
  // double l maps j into the input image
  //

  for( i=0; i< is->ysize; i++) {
    //
    // map pixel vert index to pixel index in input image
    //
    k = (i * is->height)/(double)(is->ysize) + is->y;
    tyal = yal;
    tyau = yau;
    if( (int)(k-yal) < 0 && (int)(k+yau) >= 0) {
      fprintf( stderr, "k: %f, yal: %d, yau: %d\n", k, yal, yau);
      //
      // at bottom edge.  Raise lower edge of box
      //
      while( (int)(k-tyal) < 0) tyal--;
      fprintf( stderr, "  tyal: %d\n", tyal);
    }
    if( (int)(k-tyal) < (is->inHeight) && (int)(k+yau) > (is->inHeight)-1) {
      fprintf( stderr, "k: %f, yal: %d, yau: %d\n", k, yal, yau);
      //
      // at top edge.  Lower top edge of box
      //
      while( (int)(k+tyau) > (is->inHeight)-1) tyau--;
      fprintf( stderr, "  tyau: %d\n", tyau);
    }
    
    if( (int)(k-tyal) >= 0 && (int)(k+tyau) < (is->inHeight)) {
      for( j=jmin; j<jmax; j++) {
	//
	// map pixel horz index to pixel index in intput image
	//
	l = j * is->width/(double)(is->xsize) + is->x;

	//
	// default pixel value is 0;
	//
	d = 0;
      
	if( ya <= 1 && xa <= 1) {
	  //
	  //  If we are on the orginal image, get the nearest pixel value
	  //
	  d = nearestValue( is, k, l);
	} else {
	  //
	  // Look around for the maximum value when the ouput image is
	  // being reduced
	  //
	  d = maxBox( is, buf, k, l, tyal, tyau, xal, xau);
	}
      
	if( d <= is->wval) {
	  dout = 0;
	} else {
	  if( d >= is->contrast) {
	    dout = 255;
	  } else {
	    rslt = (d - is->wval) * 255;
	    dout = rslt/(is->contrast - is->wval);
	  }
	}
      
	bp = bufo + 3*i*(is->xsize) + 3*j;
	if( d==65535) {
	  *(bp++) = 255;
	  *(bp++) = 0;
	  *bp     = 0;
	} else {
	  *(bp++)     = 255 - dout;
	  *(bp++)     = 255 - dout;
	  *(bp)       = 255 - dout;
	}
      }
    }
    
    jsp[0] = bufo + 3*i*(is->xsize);
    jpeg_write_scanlines(&cinfo, jsp, 1);
  }
  jpeg_finish_compress(&cinfo);

  //
  // don't forget to free the memory!
  //
  free( is->fullbuf);
  is->fullbuf = NULL;
  free( bufo);
}



//
// lineMaxMinAve
//
// returns mx, mn, ave of is->pw points centered on k, l
//
void lineMaxMinAve( isType *is, unsigned short *mx, unsigned short *mn, unsigned short *ave, double k, double l, double mk, double ml) {
  double x, y;
  int t;		// parameter for parametric lines
  int i;		// counter
  unsigned short p;
  double a;

  *mx = 0;
  *mn = -1;
  *ave = 0;
  a    = 0.0;
  for( t = -(is->pw)/2, i=0; i < (is->pw); i++,t++) {
    x = mk*t + k;
    y = ml*t + l;
    p = nearestValue( is, y, x);
    a += p;
    *mx = (*mx < p) ? p : *mx;
    *mn = (*mn > p) ? p : *mn;
  }
  *ave = a/i;

  return;
}


void marTiff2profile( isType *is) {
  double k, l;		// double version of row (k), column (l) indices in the input image
  double mk, bk, ml, bl;  // slope and offset of map from s to k and l
  int s;		// parametric 'index' along line
  int smin, smax;	// limit of s
  int n;		// number of points to plot
  unsigned short *buf;	// our image buffer
  unsigned short *maxs; // array of maxima from the images
  unsigned short *mins; // array of minima from the images
  unsigned short *aves; // array of averages from the images
  unsigned short mx, mn;  // max and min for graph scaling
  int err;		// return value from fprintf

  is->pad = is->pw;
  marTiffRead( is);
  buf = is->buf;

  // compute distance in input image to traverse and add one to get number of points from one end to the other
  n = sqrt( (double)(is->pbx - is->pax)*(is->pbx - is->pax) + (is->pby - is->pay)*(is->pby - is->pay)) + 1;

  maxs = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( maxs == NULL) {
    fprintf( stderr, "marTiff2profile: out of memory %d bytes\n", n * sizeof( unsigned short));
    return;
  }
  
  mins = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( mins == NULL) {
    fprintf( stderr, "marTiff2profile: out of memory %d bytes\n", n * sizeof( unsigned short));
    return;
  }
  
  aves = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( aves == NULL) {
    fprintf( stderr, "marTiff2profile: out of memory %d bytes\n", n * sizeof( unsigned short));
    return;
  }
  
  smin = 0;
  smax = n;

  mx = 0;
  mn = -1;

  mk = (double)(is->pbx - is->pax)/(double)n;
  bk = is->pax;

  ml = (double)(is->pby - is->pay)/(double)n;
  bl = is->pay;
  
  for( s=smin; s<smax; s++) {
    k = mk * s + bk;
    l = ml * s + bl;
    
    if( (int)(k+0.5) >= 0 && (int)(k+0.5) < is->inHeight && (int)(l+0.5) >=0 && (int)(l+0.5) < is->inWidth) {
      lineMaxMinAve( is, (maxs + s), (mins + s), (aves + s), k, l, -ml, mk);
    } else {
      maxs[s] = 0;
      mins[s] = 0;
      aves[s] = 0;
    }
    mx = (maxs[s] > mx) ? maxs[s] : mx;
    mn = (mins[s] < mn) ? mins[s] : mn;
  }
  free( is->fullbuf);
  
  fprintf( stderr, "<data rqid=\"%s\" xMin=\"%d\" xMax=\"%d\" yMin=\"%d\" yMax=\"%d\">\n", is->rqid, smin, smax, mn, mx);

  err = fprintf( is->fout, "<data rqid=\"%s\" xMin=\"%d\" xMax=\"%d\" yMin=\"%d\" yMax=\"%d\">\n", is->rqid, smin, smax, mn, mx);
  if( err < 0) {
    fprintf( stderr, "marTiff2profile: write failed: '%s'\n", strerror( errno));
    return;
  }
  for( s=smin; s<smax; s++) {
    fprintf( stdout, "<point x=\"%d\" min=\"%u\" ave=\"%u\" max=\"%u\"/>\n", s, mins[s], aves[s], maxs[s]);

    err = fprintf( is->fout, "<point x=\"%d\" min=\"%u\" ave=\"%u\" max=\"%u\"/>\n", s, mins[s], aves[s], maxs[s]);

    if( err < 0) {
      fprintf( stderr, "marTiff2profile: write failed: '%s'\n", strerror( errno));
      return;
    }
  }
  fprintf( stderr, "</data>\n");
  err = fprintf( is->fout, "</data>\n");
  if( err < 0) {
    fprintf( stderr, "marTiff2profile: write failed: '%s'\n", strerror( errno));
    return;
  }

  fflush( is->fout);
}
