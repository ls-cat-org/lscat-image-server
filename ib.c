/*
** ib.c
**
** Routines that operate on the image buffer
**
** Copyright (C) 2009-2010 by Keith Brister
** All rights reserved.
**
*/

#include "is.h"

unsigned short nearestValue( isType *is, double k, double l) {
  return *(is->b->buf + (int)(k+0.5)*(is->b->inWidth)+(int)(l+0.5));
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
      d1 = *(buf + m*(is->b->inWidth) + n);
      d = (d>d1 ? d : d1);
    }
  }

  return d;
}

void jerror_handler( j_common_ptr cp) {
  fprintf( stderr, "Is that socket still there?  I think not!\n");  
  longjmp( *(jmp_buf *)cp->client_data, 1);
}

void ib2jpeg( isType *is ) {
  struct jpeg_compress_struct cinfo;
  int dataRead;
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
  sigset_t sigset;

  //
  // Ignore sigpipe
  // This forces the jpeg error handler to
  // deal with bad sockets
  //
  sigemptyset( &sigset);
  sigaddset( &sigset, SIGPIPE);
  sigprocmask( SIG_BLOCK, &sigset, NULL);

  // Make sure we have data
  pthread_mutex_lock( &ibUseMutex);
  dataRead = is->b->dataRead;
  if( dataRead==0) {
    is->b->dataRead = 1;
    pthread_rwlock_wrlock( &(is->b->datalock));
  }
  pthread_mutex_unlock( &ibUseMutex);

  if( dataRead == 0) {
    is->b->getData( is);
    pthread_rwlock_unlock( &(is->b->datalock));
  }
  // now we know there is some data
  pthread_rwlock_rdlock( &(is->b->datalock));


  //
  // Setup the jpeg stuff
  //
  cinfoSetup = 0;
  buf = NULL;
  bufo = NULL;

  if( setjmp( j_jumpHere)) {
    if( cinfoSetup)
      jpeg_destroy_compress( &cinfo);
    if( bufo != NULL) {
      free( bufo);
      bufo = NULL;
    }

    pthread_rwlock_unlock( &(is->b->datalock));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    return;
  }
  

  // Refuse to make really, really, really small images
  //
  is->xsize = is->xsize < 8 ? 8 : is->xsize;
  is->ysize = is->ysize < 8 ? 8 : is->ysize;


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

  // Make sure that xal, xau, yal, and yau are all <= the image pad
  xal = xal > ISPADSIZE ? ISPADSIZE : xal;
  xau = xau > ISPADSIZE ? ISPADSIZE : xau;

  yal = yal > ISPADSIZE ? ISPADSIZE : yal;
  yau = yau > ISPADSIZE ? ISPADSIZE : yau;


  //
  // Data should already be here
  //
  buf = is->b->buf;

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
    fprintf( stderr, "ib2jpeg: Out of memory.  calloc(%lu) failed\n", 3*is->ysize*is->xsize*sizeof( unsigned char));
    pthread_rwlock_unlock( &(is->b->datalock));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    return;
  }

  //
  // compute the range of j, ignoring for now the j's that, considering the box, would lead to pixels off the input image
  //

  jmin = -(is->x)*(is->xsize)/(is->width);
  jmax = ((is->b->inWidth)-(is->x)) * (is->xsize)/(is->width);
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
    if( (int)(k-tyal) < (is->b->inHeight) && (int)(k+yau) > (is->b->inHeight)-1) {
      fprintf( stderr, "k: %f, yal: %d, yau: %d\n", k, yal, yau);
      //
      // at top edge.  Lower top edge of box
      //
      while( (int)(k+tyau) > (is->b->inHeight)-1) tyau--;
      fprintf( stderr, "  tyau: %d\n", tyau);
    }
    
    if( (int)(k-tyal) >= 0 && (int)(k+tyau) < (is->b->inHeight)) {
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
  if( bufo != NULL) {
    free( bufo);
    bufo = NULL;
  }
  pthread_rwlock_unlock( &(is->b->datalock));
  close( is->fd);
  is->fd = -1;
  is->fout = NULL;
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
  int ll, ul;		// lower limit and upper limit of line

  ll = -(is->pw/2) < -ISPADSIZE ? -ISPADSIZE : -(is->pw/2);
  ul = is->pw + ll > ISPADSIZE ? ISPADSIZE : is->pw;

  *mx = 0;
  *mn = -1;
  *ave = 0;
  a    = 0.0;
  for( t = ll, i=0; i < ul; i++,t++) {
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


void ib2profile( isType *is) {
  int dataRead;
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

  // Make sure we have data
  pthread_mutex_lock( &ibUseMutex);
  dataRead = is->b->dataRead;
  if( dataRead==0) {
    is->b->dataRead = 1;
    pthread_rwlock_wrlock( &(is->b->datalock));
  }
  pthread_mutex_unlock( &ibUseMutex);

  if( dataRead == 0) {
    is->b->getData( is);
    pthread_rwlock_unlock( &(is->b->datalock));
  }
  // now we know there is some data
  pthread_rwlock_rdlock( &(is->b->datalock));

  buf = is->b->buf;

  // compute distance in input image to traverse and add one to get number of points from one end to the other
  n = sqrt( (double)(is->pbx - is->pax)*(is->pbx - is->pax) + (is->pby - is->pay)*(is->pby - is->pay)) + 1;

  maxs = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( maxs == NULL) {
    fprintf( stderr, "ib2profile: out of memory %lu bytes\n", n * sizeof( unsigned short));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    pthread_rwlock_unlock( &(is->b->datalock));
    return;
  }
  
  mins = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( mins == NULL) {
    fprintf( stderr, "ib2profile: out of memory %lu bytes\n", n * sizeof( unsigned short));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    pthread_rwlock_unlock( &(is->b->datalock));
    return;
  }
  
  aves = (unsigned short *)calloc( n, sizeof( unsigned short));
  if( aves == NULL) {
    fprintf( stderr, "ib2profile: out of memory %lu bytes\n", n * sizeof( unsigned short));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    pthread_rwlock_unlock( &(is->b->datalock));
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
    
    if( (int)(k+0.5) >= 0 && (int)(k+0.5) < is->b->inHeight && (int)(l+0.5) >=0 && (int)(l+0.5) < is->b->inWidth) {
      lineMaxMinAve( is, (maxs + s), (mins + s), (aves + s), k, l, -ml, mk);
    } else {
      maxs[s] = 0;
      mins[s] = 0;
      aves[s] = 0;
    }
    mx = (maxs[s] > mx) ? maxs[s] : mx;
    mn = (mins[s] < mn) ? mins[s] : mn;
  }
  
  fprintf( stderr, "<data rqid=\"%s\" xMin=\"%d\" xMax=\"%d\" yMin=\"%d\" yMax=\"%d\">\n", is->rqid, smin, smax, mn, mx);

  err = fprintf( is->fout, "<data rqid=\"%s\" xMin=\"%d\" xMax=\"%d\" yMin=\"%d\" yMax=\"%d\">\n", is->rqid, smin, smax, mn, mx);
  if( err < 0) {
    fprintf( stderr, "ib2profile: write failed: '%s'\n", strerror( errno));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    pthread_rwlock_unlock( &(is->b->datalock));
    return;
  }
  for( s=smin; s<smax; s++) {
    fprintf( stdout, "<point x=\"%d\" min=\"%u\" ave=\"%u\" max=\"%u\"/>\n", s, mins[s], aves[s], maxs[s]);

    err = fprintf( is->fout, "<point x=\"%d\" min=\"%u\" ave=\"%u\" max=\"%u\"/>\n", s, mins[s], aves[s], maxs[s]);

    if( err < 0) {
      fprintf( stderr, "ib2profile: write failed: '%s'\n", strerror( errno));
      close( is->fd);
      is->fd = -1;
      is->fout = NULL;
      pthread_rwlock_unlock( &(is->b->datalock));
      return;
    }
  }
  fprintf( stderr, "</data>\n");
  err = fprintf( is->fout, "</data>\n");
  if( err < 0) {
    fprintf( stderr, "ib2profile: write failed: '%s'\n", strerror( errno));
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    pthread_rwlock_unlock( &(is->b->datalock));
    return;
  }

  fflush( is->fout);
  close( is->fd);
  is->fd = -1;
  is->fout = NULL;
  pthread_rwlock_unlock( &(is->b->datalock));
}


void ib2header( isType *is) {
  int dataread;

  fprintf( is->fout, "<imageheader");

  fprintf( is->fout, " success=\"true\"");
  fprintf( is->fout, " rqid=\"%s\"",	          is->rqid);
  fprintf( is->fout, " filename=\"%s\"",          is->b->h_filename);
  fprintf( is->fout, " dir=\"%s\"",               is->b->h_dir);
  fprintf( is->fout, " detector=\"%s\"",	  is->b->h_detector);
  fprintf( is->fout, " beamline=\"%s\"",	  is->b->h_beamline);
  fprintf( is->fout, " dist=\"%.3f\"",            is->b->h_dist);
  fprintf( is->fout, " rotationRange=\"%.3f\"",   is->b->h_rotationRange);
  fprintf( is->fout, " startPhi=\"%.3f\"",        is->b->h_startPhi);
  fprintf( is->fout, " wavelength=\"%.5f\"",      is->b->h_wavelength);
  fprintf( is->fout, " beamX=\"%.3f\"",           is->b->h_beamX);
  fprintf( is->fout, " beamY=\"%.3f\"",           is->b->h_beamY);
  fprintf( is->fout, " imagesizeX=\"%d\"",        is->b->h_imagesizeX);
  fprintf( is->fout, " imagesizeY=\"%d\"",        is->b->h_imagesizeY);
  fprintf( is->fout, " pixelsizeX=\"%.3f\"",      is->b->h_pixelsizeX);
  fprintf( is->fout, " pixelsizeY=\"%.3f\"",      is->b->h_pixelsizeY);
  fprintf( is->fout, " integrationTime=\"%.3f\"", is->b->h_integrationTime);
  fprintf( is->fout, " exposureTime=\"%.3f\"",    is->b->h_exposureTime);
  fprintf( is->fout, " readoutTime=\"%.3f\"",     is->b->h_readoutTime);
  fprintf( is->fout, " saturation=\"%d\"",        is->b->h_saturation);
  fprintf( is->fout, " minValue=\"%d\"",          is->b->h_minValue);
  fprintf( is->fout, " maxValue=\"%d\"",          is->b->h_maxValue);
  fprintf( is->fout, " meanValue=\"%.1f\"",       is->b->h_meanValue);
  fprintf( is->fout, " rmsValue=\"%.1f\"",        is->b->h_rmsValue);
  fprintf( is->fout, " nSaturated=\"%d\"",        is->b->h_nSaturated);
  fprintf( is->fout, "/>\n");
  fflush( is->fout);
  close( is->fd);
  is->fd = -1;
  is->fout = NULL;

  // Now that the header is done, let's read the data too, unless someone else beat us to it

  pthread_mutex_lock( &ibUseMutex);
  dataread = is->b->dataRead;
  if( dataread == 0) {
    is->b->dataRead = 1;
    pthread_rwlock_wrlock( &(is->b->datalock));
  }
  pthread_mutex_unlock( &ibUseMutex);

  if( dataread == 0) {
    is->b->getData( is);
    pthread_rwlock_unlock( &(is->b->datalock));
  }
}

void ib2download( isType *is) {
  int fd;
  int cnt;
  int wcnt;
  char buf[1024];

  //
  // Don't need data or header, just user and groups (and file name)
  //

  if( fork() != 0) {
    //
    // In Parent
    //
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    return;
  }
  //
  // In Child
  //
  setgid( is->gid);
  setuid( is->uid);
  fd = open( is->fn, O_RDONLY);
  while( 1) {
    cnt = read( fd, buf, sizeof(buf));
    if( cnt <= 0)
      break;
    wcnt = write( is->fd, buf, cnt);
    if( cnt != wcnt)
      break;
  }
  close( fd);
  close( is->fd);
  exit( 0);
}

void ib2tarball( isType *is) {
  //
  // Don't need data or header, just user, groups, and dspid
  //
  if( fork() != 0) {
    //
    // In Parent
    //
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    return;
  }
  //
  // In Child
  //
  // Prevent death of the parent from killing us too
  //
  {
    struct sigaction sa;

    sa.sa_handler = SIG_IGN;
    sigemptyset( &sa.sa_mask);
    sa.sa_flags = 0;
    sigaction( SIGHUP, &sa, NULL);

    //
    // Become the user
    //
    setgid( is->gid);
    setuid( is->uid);

    //
    // fix stdout
    //
    close( 1);		// close stdout
    dup( is->fd);	// the new stdout is at hand

    //
    // This is where we exec into the tarball script
    //
    execl( "/pf/bin/lsMakeDataTar.py", "/pf/bin/lsMakeDataTar.py", is->dspid, (const char *)NULL);

    close( is->fd);
    exit( 0);
  }
}


void ib2indexing( isType *is) {
  int chld;
  int status;
  //
  // Don't need data or header, just user and groups (and file names ifn1 and ifn2)
  //

  if( (chld=fork()) != 0) {
    //
    // In Parent
    //
    close( is->fd);
    is->fd = -1;
    is->fout = NULL;
    waitpid( chld, &status, 0);
    return;
  }
  //
  // In Child
  //
  setgid( is->gid);
  setuid( is->uid);

  close( 1);	// close stdout
  dup( is->fd);	// make our file descriptor the new stdout

  chdir( is->hd);	// go to the user's home directory
  

  //
  // Call the indexing routine
  //
  execl( "/pf/bin/lsIndexing.py", "/pf/bin/lsIndexing.py", is->ifn1, is->ifn2, is->b->h_detector, (const char *)NULL);

  close( is->fd);
  exit( 0);
}

