#include "is.h"

/*
** returns the maximum value of ha xa by ya box centered on d,l
*/
uint32_t maxBox16( uint32_t *badPixels, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  int m, n;
  uint32_t d, d1;
  uint16_t *bp = (uint16_t *) buf;

  d = 0;

  for (m=k-yal; m < k+yau; m++) {
    //
    // Don't roll off the top or bottom of the original image
    //
    if (m < 0 || m >= bufHeight)
      continue;
    for (n=l-xal; n<l+xau; n++) {
      //
      // Don't fall off the right or the left of the original image
      //
      if (n < 0 || n >= bufWidth)
        continue;

      // Check for known bad pixel
      if ( badPixels && *(badPixels + m*bufWidth + n))
        continue;

      d1 = *(bp + m*bufWidth + n);

      //
      // see if we have a saturated pixel
      if (d1 == 0xffff)
        return 0xffffffff;

      d = (d>d1 ? d : d1);
    }
  }
  return d;
}


/*
** returns the maximum value of ha xa by ya box centered on d,l
*/
uint32_t maxBox32( uint32_t *badPixels, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  int m, n;
  uint32_t d, d1;
  uint32_t *bp = (uint32_t *)buf;
  
  d = 0;
  
  for (m=k-yal; m < k+yau; m++) {
    //
    // Don't roll off the top or bottom of the original image
    //
    if (m < 0 || m >= bufHeight)
      continue;
    
    for (n=l-xal; n<l+xau; n++) {
      //
      // Don't fall off the right or the left of the orininal image
      //
      if (n < 0 || n >= bufWidth)
        continue;
      
      // Check for a known bad pixel
      if (badPixels && *(badPixels + m*bufWidth + n))
        continue;

      d1 = *(bp + m*bufWidth + n);
      
      //
      // Check for a saturated pixel
      // Can't get a higher count than this, might as well return
      //
      if (d1 == 0xffffffff)
        return d1;

      d = (d>d1 ? d : d1);
    }
  }
  return d;
}

uint32_t nearest16( uint32_t *badPixels, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  uint16_t *bp = (uint16_t *)buf;
  uint32_t rtn;
  int index;

  index = (int)(k+0.5)*bufWidth + (int)(l+0.5);
  if (badPixels && *(badPixels + index)) {
    rtn = 0;
  } else {
    rtn = *(bp + index);
  }

  return rtn;
}

uint32_t nearest32( uint32_t *badPixels, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  int index;
  uint32_t *bp = (uint32_t *)buf;
  uint32_t rtn;

  index = (int)(k+0.5)*bufWidth + (int)(l+0.5);
  if (badPixels && *(badPixels + index)) {
    rtn = 0;
  } else {
    rtn = *(bp + index);
  }

  return rtn;
}

void reduceImage16( isImageBufType *src, isImageBufType *dst, int x, int y, int winWidth, int winHeight) {

  uint32_t (*cvtFunc)(uint32_t *, void *, int, int, double, double, int, int, int, int);

  int row=0, col=0;
  int xa, ya;
  int xal, yal, xau, yau;
  uint32_t pxl;
  double d_row, d_col;
  uint16_t *srcBuf;
  uint16_t *dstBuf;
  int dstWidth;
  int dstHeight;
  int srcWidth;
  int srcHeight;

  srcBuf = src->buf;
  dstBuf = dst->buf;

  dstWidth  = dst->buf_width;
  dstHeight = dst->buf_height;

  srcWidth  = src->buf_width;
  srcHeight = src->buf_height;

  //
  // size of rectangle to search for the maximum pixel value
  // yal and xal are subtracted from ya and xa for the lower bound of the box and
  // yau and xau are added to ya and xa for the upper bound of the box
  //
  xa = (winWidth)/(dstWidth);
  xal = xau = xa/2;
  if( (xal + xau) < xa)
    xau++;

  ya = (winHeight)/(dstHeight);
  yal = yau = ya/2;
  if ((yal + yau) < ya)
    yau++;

  cvtFunc = NULL;
  if (xa <= 1 || ya <= 1) {
    cvtFunc = nearest16;
  } else {
    cvtFunc = maxBox16;
  }

  for (row=0; row<dstHeight; row++) {
    // "index" of vertical position on original image
    d_row = row * winHeight/(double)(dstHeight) + y;

    for (col=0; col<dstWidth; col++) {
      // "index" of the horizontal position on the original image
      d_col = col * winWidth/(double)(dstWidth) + x;

      pxl = cvtFunc( src->bad_pixel_map, srcBuf, srcWidth, srcHeight, d_row, d_col, yal, yau, xal, xau);
      
      *(dstBuf + row*dstWidth + col) = pxl;
    }
  }
}

/** Image reduction is defined by a "zoom" and a "sector".
 *
 *  The width and heigh of the original image are divided by "zoom"
 *  and the resulting segments addressed by column and row indices
 *  starting with the upper left hand corner.  For example:
 *
 *  Zoom: 4 gives 16 segments from [0,0] to [3,3].  Zoom: 1.5 gives 4
 *  segments from [0,] to [1,1] where the right half of [0,1] is
 *  blank, the bottom half of [0,1] is blank, and only the upper left
 *  hand quadrant of [1,1] is potentially non-blank.
 *
 *
 *  Call with
 *
 *    @param rc redisContext* Open redis context to local redis server
 *
 *    @param job.fn     string   File name of the data we are interested in
 *    @param job.frame  integer  Requested frame.  Default is 1
 *    @param job.zoom   double   Ratio of full source image to the portion of the source image we are processing. Zoom will be rounded to the nearest 0.1
 *    @param job.segcol integer  See above for discussion of col/row/zoom
 *    @param job.segrow integer  
 *    @param job.xsize  integer  Width of output image in pixels
 *    @param job.ysize  integer  Height of output image in pixels
 *
 *
 *  Return with
 *
 *    read locked buffer
 */
isImageBufType *isReduceImage(isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isReducedImage";
  isImageBufType *rtn;
  isImageBufType *raw;
  double zoom;
  int segcol;
  int segrow;
  int seglen;
  const char *fn;
  int frame;
  char *reducedKey;
  int gid;
  int reducedKeyStrlen;


  int srcWidth;
  int srcHeight;
  int image_depth;
  int x;
  int y;
  int winWidth = json_integer_value(json_object_get(job, "width"));                             // width of input image to map to output image
  int winHeight = json_integer_value(json_object_get(job, "height"));                           // height of input image to map to output image
  int dstWidth  = json_integer_value(json_object_get(job, "xsize"));                            // width, in pixels, of output image
  int dstHeight = json_integer_value(json_object_get(job, "ysize"));                            // height, in pixels, of output image

  fn    = json_string_value(json_object_get(job, "fn"));
  frame = json_integer_value(json_object_get(job, "frame"));

  zoom   = json_real_value(json_object_get(job, "zoom"));
  segcol = json_integer_value(json_object_get(job, "segcol"));
  segrow = json_integer_value(json_object_get(job, "segrow"));
  
  //
  // Reality check on zoom
  //
  zoom = (floor(10.0*zoom+0.5))/10.0;   // round to the nearest 0.1
  if (zoom <= 1.0) {
    zoom = 1.0;
    segcol = 0;
    segrow = 0;
  }
  
  //
  // Reality check on segcol and segrow
  //
  seglen = ceil(zoom);
  segcol = segcol > seglen - 1 ? seglen - 1 : segcol;
  segrow = segrow > seglen - 1 ? seglen - 1 : segrow;
  
  //
  // Refuse really tiny conversions
  dstWidth  = dstWidth  < 8 ? 8 : dstWidth;
  dstHeight = dstHeight < 8 ? 8 : dstHeight;

  //
  // Reality check on the destination image size to keep weirdos from
  // finding buffer overflows.
  //
  if (dstWidth > 10000 || dstHeight > 10000) {
    fprintf(stderr, "%s: unlikely valid destination size %d X %d\n", id, dstWidth, dstHeight);
    return NULL;
  }

  if (fn == NULL) {
    fprintf(stderr, "%s: Cannot find file name in job\n", id);
    return NULL;
  }
  //
  // Reality check on frame number
  //
  frame = frame <= 0 ? 1 : frame;

  gid = getegid();
  //
  // Reality check on gid
  //
  if (gid < 9000) {
    fprintf(stderr, "%s: Unlikely gid %d\n", id, gid);
    return NULL;
  }

  // Instead of calculating the exact string length we'll guess a
  // value that's too big.  We've not set an upper limit on the frame
  // as there may be some legitimate reasons not to set such an fixed
  // upper bound.  Instead we calculate the space needed for that.
  //
  reducedKeyStrlen = strlen(fn) + (int)log10(frame) + 1 + 128;
  reducedKey = calloc(1, reducedKeyStrlen + 1);
  if (reducedKey == NULL) {
    fprintf(stderr, "%s: Out of memory (reducedKey)\n", id);
    exit (-1);
  }
  snprintf(reducedKey, reducedKeyStrlen, "%d:%s-%d-%0.1f-%d-%d-%d-%d",
           getegid(), fn, frame, zoom, segcol, segrow, dstWidth, dstHeight);
  reducedKey[reducedKeyStrlen] = 0;
 
  fprintf(stderr, "%s: about to get image buf from key %s\n", id, reducedKey);

  rtn = isGetImageBufFromKey(ibctx, rc, reducedKey);

  free(reducedKey);

  if (rtn == NULL || rtn->buf != NULL) {
    //
    // We either failed completely or succeeded without really trying.
    // Either way we are done here.  When rtn is not null the buffer
    // is read locked.  Don't forget to release it.
    //
    fprintf(stderr, "%s: returned %s\n", id, rtn == NULL ? "Failed to get data, giving up." : rtn->key);
  
    return rtn;
  }

  //
  // Here we have a write locked buffer (with in_use = 1) with nothing in it.
  //
  
  // Get the unreduced file
  fprintf(stderr, "%s: Getting raw data for %s\n", id, rtn->key);
  raw = isGetRawImageBuf(ibctx, rc, job);
  if (raw == NULL) {
    fprintf(stderr, "%s: Failed to get raw data for %s\n", id, rtn->key);
    //
    // Can't fill the buffer we want, should probably raise some kind
    // of hell.  Presumably isGetRawImageBuf complained to the
    // authorities.
    //
    pthread_rwlock_unlock(&rtn->buflock);
    fprintf(stderr, "%s: Gave up lock for key %s\n", id, reducedKey);
    
    fprintf(stderr, "%s: Waiting for ctxMutex\n", id);
    pthread_mutex_lock(&ibctx->ctxMutex);
    fprintf(stderr, "%s: Got ctxMutex\n", id);
    rtn->in_use--;
    pthread_mutex_unlock(&ibctx->ctxMutex);
    fprintf(stderr, "%s: Unlocked ctxMutex\n", id);
    return NULL;
  }
  
  fprintf(stderr, "%s: Got raw data for %s\n", id, rtn->key);
  // raw is the the raw data we'll be reducing.  rtn is the reduced
  // buffer we'll be filling.
  // 
  // Here raw is read locked and rtn is write locked.
  //
  srcWidth  = json_integer_value(json_object_get(raw->meta, "x_pixels_in_detector"));       // width, in pixels, of full input image
  srcHeight = json_integer_value(json_object_get(raw->meta, "y_pixels_in_detector"));       // height, in pixels, of full input image
  
  image_depth = json_integer_value(json_object_get(raw->meta, "image_depth"));
  if (image_depth != 2 && image_depth != 4) {
    fprintf(stderr, "%s: bad image depth %d.  Likely this is a serious error somewhere\n", id, image_depth);
    exit (-1);
  }

  winWidth  = srcWidth / zoom;
  winHeight = srcHeight / zoom;

  rtn->buf_size = dstWidth * dstHeight * image_depth;
  rtn->buf = calloc(1, rtn->buf_size);
  if (rtn->buf == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->buf_width  = dstWidth;
  rtn->buf_height = dstHeight;
  rtn->buf_depth  = image_depth;

  rtn->meta = raw->meta;
  json_incref(rtn->meta);
  rtn->meta_str = json_dumps(rtn->meta, JSON_COMPACT | JSON_SORT_KEYS | JSON_INDENT(0));

  x = winWidth  * segcol;
  y = winHeight * segrow;
  
  switch (image_depth) {
  case 2:
    reduceImage16(raw, rtn, x, y, winWidth, winHeight);
    break;

  case 4:
    reduceImage16(raw, rtn, x, y, winWidth, winHeight);
    break;

  default:
    fprintf(stderr, "%s: Unusable image depth %d\n", id, image_depth);
    exit (-1);
  }

  pthread_rwlock_unlock(&raw->buflock);
  fprintf(stderr, "%s: Gave up lock for key %s\n", id, reducedKey);

  // We don't need the raw buffer anymore
  fprintf(stderr, "%s: Waiting for ctxMutex\n", id);
  pthread_mutex_lock(&ibctx->ctxMutex);
  fprintf(stderr, "%s: Got ctxMutex\n", id);
  raw->in_use--;
  pthread_mutex_unlock(&ibctx->ctxMutex);
  fprintf(stderr, "%s: Unlocked ctxMutex\n", id);

  //
  // Exchange our write lock for a read lock to let our other threads get to work.
  //
  pthread_rwlock_unlock(&rtn->buflock);
  fprintf(stderr, "%s: Gave up lock for key %s\n", id, reducedKey);

  fprintf(stderr, "%s: Getting read lock for key %s\n", id, rtn->key);
  pthread_rwlock_rdlock(&rtn->buflock);
  fprintf(stderr, "%s: Got read lock for key %s\n", id, rtn->key);

  //
  // Let the other processes get started on this one too.
  //
  isWriteImageBufToRedis(rtn, rc);

  return rtn;
}  


