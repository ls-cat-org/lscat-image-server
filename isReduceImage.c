/*! @file isReduceImage.c
 *  @copyright 2017 by Northwestern University All Rights Reserved
 *  @author Keith Brister
 *  @brief Reduce full images for the LS-CAT Image Server Version 2
 */
#include "is.h"

/**
 **
 **
 */
void set_up_bins(isImageBufType *src, isImageBufType *dst, double winWidth, double winHeight, int x, int y) {
  static const char *id = FILEID "set_up_bins";
  
  double bin_width;
  bin_t *bp;
  int i;
  double beam_center_x, beam_center_y;
  double box_w, box_h;
  double dst_center_x;
  double dst_center_y;
  double min_dest_dist2;
  double max_dest_dist2;
  double tmp_dest_dist2;
  double dstWidth;
  double dstHeight;

  beam_center_x        = get_double_from_json_object( id, src->meta, "beam_center_x");
  beam_center_y        = get_double_from_json_object( id, src->meta, "beam_center_y");

  dstWidth  = dst->buf_width;
  dstHeight = dst->buf_height;

  //
  // Calculate ratio of width and height to the original image so we
  // can scale the beam center to the new image.
  //
  box_w = winWidth / (double)dst->buf_width;
  box_h = winHeight / (double)dst->buf_height;

  //
  // Find the beam center in the destination image space
  //
  // This point is not necessarily on the image itself as the detector
  // may be offset.
  //
  dst_center_x = (beam_center_x - x) / box_w;
  dst_center_y = (beam_center_y - y) / box_h;

  dst->beam_center_x = dst_center_x;
  dst->beam_center_y = dst_center_y;

  //
  // Find the range of distances on the destination images.  First
  // assume the beam center is not on the image.
  //

  min_dest_dist2 = dst_center_x * dst_center_x + dst_center_y * dst_center_y;
  max_dest_dist2 = min_dest_dist2;

  tmp_dest_dist2 = (dstWidth - dst_center_x) * (dstWidth - dst_center_x) + dst_center_y * dst_center_y; // URH
  if (tmp_dest_dist2 < min_dest_dist2)
    min_dest_dist2 = tmp_dest_dist2;
  if (tmp_dest_dist2 > max_dest_dist2)
    max_dest_dist2 = tmp_dest_dist2;

  tmp_dest_dist2 = dst_center_x * dst_center_x + (dstHeight - dst_center_y) * (dstHeight - dst_center_y);  // LLH
  if (tmp_dest_dist2 < min_dest_dist2)
    min_dest_dist2 = tmp_dest_dist2;
  if (tmp_dest_dist2 > max_dest_dist2)
    max_dest_dist2 = tmp_dest_dist2;
    
  tmp_dest_dist2 = (dstWidth - dst_center_x) * (dstWidth - dst_center_x) +  (dstHeight - dst_center_y) * (dstHeight - dst_center_y);  // LLH
  if (tmp_dest_dist2 < min_dest_dist2)
    min_dest_dist2 = tmp_dest_dist2;
  if (tmp_dest_dist2 > max_dest_dist2)
    max_dest_dist2 = tmp_dest_dist2;

  if (dst_center_x >= 0 && dst_center_x < dstWidth &&
      dst_center_y >= 0 && dst_center_y < dstHeight) {

    // our beam is on the destination image (this should be normally
    // the case)
    min_dest_dist2 = 0.0;
  }

  dst->min_dist2 = min_dest_dist2;
  dst->max_dist2 = max_dest_dist2;

  //
  bin_width = (max_dest_dist2 - min_dest_dist2) / IS_OUTPUT_IMAGE_BINS;

  //
  // the size of the bins array is OUTPUT_IMAGE_BINS + 1 with
  // bins[OUTPUT_IMAGE_BINS] reserved for ice ring statistics.  This
  // may be a bit of kludge but it keeps all this ice calculation
  // stuff in get_bin_number.
  //

  for (i=0; i<=IS_OUTPUT_IMAGE_BINS; i++) {
    bp = &(dst->bins[i]);
    bp->dist2_low   = i * bin_width + min_dest_dist2;
    bp->dist2_high  = bp->dist2_low + bin_width;
    bp->ice_ring_list = NULL;

    bp->rms     = 0.0;
    bp->mean    = 0.0;
    bp->sd      = 0.0;
    bp->min     = 0xffffffff;
    bp->min_row = 0;
    bp->min_col = 0;
    bp->max     = 0;
    bp->max_row = 0;
    bp->max_col = 0;
    bp->n       = 0;
    bp->sum     = 0.0;
    bp->sum2    = 0.0;
  }
}

int get_bin_number( isImageBufType *dst, int col, int row) {
  ice_ring_list_t *irp;
  int rtn;
  double dist2;

  dist2 = ( col - dst->beam_center_x) * (col - dst->beam_center_x) + (row - dst->beam_center_y) * (row - dst->beam_center_y);
  
  rtn = (double)(IS_OUTPUT_IMAGE_BINS) / (dst->max_dist2 - dst->min_dist2) * (dist2 - dst->min_dist2);
  
  if (rtn < 0) {
    rtn = 0;
  }

  if (rtn >= IS_OUTPUT_IMAGE_BINS) {
    rtn = IS_OUTPUT_IMAGE_BINS - 1;
  }

  for (irp=dst->bins[rtn].ice_ring_list; irp != NULL; irp = irp->next) {
    if (dist2 >= irp->dist2_low && dist2 <= irp->dist2_high) {
      rtn = IS_OUTPUT_IMAGE_BINS;  // special bin reserved for ice rings
      break;
    }
  }

  return rtn;
}


void add_to_stats(isImageBufType *dst, int row, int col, uint32_t pix) {
  static const char *id = FILEID "add_to_stats";
  int bin;

  (void) id;

  bin = get_bin_number(dst, col, row);

  dst->bins[bin].n++;
  dst->bins[bin].sum += pix;
  dst->bins[bin].sum2 += pix*pix;

  if (pix < dst->bins[bin].min) {
    dst->bins[bin].min = pix;
    dst->bins[bin].min_row = row;
    dst->bins[bin].min_col = col;
  }

  if (pix > dst->bins[bin].max) {
    dst->bins[bin].max = pix;
    dst->bins[bin].max_row = row;
    dst->bins[bin].max_col = col;
  }
}

void calc_stats(isImageBufType *dst) {
  static const char *id = FILEID "calc_stats";
  int i;
  int n;
  double mean;
  double rms;
  double sd;
  double sum;
  double sum2;
  uint32_t min;
  uint32_t max;

  n    = 0;
  mean = 0.0;
  rms  = 0.0;
  sd   = 0.0;
  sum  = 0.0;
  sum2 = 0.0;
  max  = 0;
  min  = 0xffffffff;

  for (i=0; i <= IS_OUTPUT_IMAGE_BINS; i++) {
    if (dst->bins[i].n == 0) {
      dst->bins[i].mean = 0;
      dst->bins[i].rms  = 0;
      dst->bins[i].sd   = 0;
      continue;
    }

    n    += dst->bins[i].n;
    sum  += dst->bins[i].sum;
    sum2 += dst->bins[i].sum2;

    dst->bins[i].mean = dst->bins[i].sum / dst->bins[i].n;
    dst->bins[i].rms  = sqrt(dst->bins[i].sum2 / dst->bins[i].n);
    dst->bins[i].sd   = sqrt(dst->bins[i].sum2 / dst->bins[i].n - dst->bins[i].mean * dst->bins[i].mean);

    if (min > dst->bins[i].min) {
      min = dst->bins[i].min;
    }

    if (max < dst->bins[i].max) {
      max = dst->bins[i].max;
    }
  }

  if (n > 0) {
    mean = sum / n;
    rms  = sqrt(sum2 / n);
    sd   = sqrt(sum2 / n - mean * mean);
  } else {
    mean = 0;
    rms = 0;
    sd  = 0;
  } 

  fprintf(stdout, "%s: n: %d  mean: %f, min: %d, max: %d, rms: %f  stddev: %f\n",
          id, n, mean, min, max, rms, sd);


  set_json_object_integer(id, dst->meta, "n",      n);
  set_json_object_real(id, dst->meta,    "mean",   mean);
  set_json_object_integer(id, dst->meta, "min",    min);
  set_json_object_integer(id, dst->meta, "max",    max);
  set_json_object_real(id, dst->meta,    "rms",    rms);
  set_json_object_real(id, dst->meta,    "stddev", sd);
}

/** For 16 bit images, this returns the maximum value of ha xa by ya box centered on (k,l).
 ** 
 ** @param badPixels     Our bad pixel map
 **
 ** @param buf           Buffer contianing our image
 **
 ** @param bufWidth      image width
 ** 
 ** @param bufHeight     image height
 **
 ** @param k             index along height around which to find the max
 **
 ** @param l             index along width around which to find the max
 **
 ** @param yal           box extends this distance above k
 **
 ** @param yau           box extends this distance below k
 **
 ** @param xal           box extends this distance to the left of l
 **
 ** @param xau           box extens this distance to the right of l
 **
 ** @returns Maximum value found in the box.  Bad pixels are ignored.
 */
uint32_t maxBox16( uint32_t *badPixels, uint32_t *minp, int *nsatp, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  static const char *id = FILEID "maxBox16";
  int m, n;
  uint32_t d, d1;
  uint16_t *bp = (uint16_t *) buf;

  (void)id;

  d      = 0;
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
      if (d1 == 0xffff) {
        (*nsatp)++;
      }

      d = (d>d1 ? d : d1);

      if (*minp > d) {
        *minp = d;
      }
    }
  }
  return d == 0xffff ? 0xffffffff : d;
}


/** For 32 bit images, this returns the maximum value of ha xa by ya box centered on (k,l).
 ** 
 ** @param badPixels     Our bad pixel map
 **
 ** @param buf           Buffer contianing our image
 **
 ** @param bufWidth      image width
 ** 
 ** @param bufHeight     image height
 **
 ** @param k             index along height around which to find the max
 **
 ** @param l             index along width around which to find the max
 **
 ** @param yal           box extends this distance above k
 **
 ** @param yau           box extends this distance below k
 **
 ** @param xal           box extends this distance to the left of l
 **
 ** @param xau           box extens this distance to the right of l
 **
 ** @returns Maximum value found in the box.  Bad pixels are ignored.
 */
uint32_t maxBox32( uint32_t *badPixels, uint32_t *minp, int *nsatp, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  static const char *id = FILEID "maxBox32";
  int m, n;
  uint32_t d, d1;
  uint32_t *bp = (uint32_t *)buf;
  
  (void)id;
  
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
      if (d1 == 0xffffffff) {
        (*nsatp)++;
      }

      d = d>d1 ? d : d1;

      if (*minp > d) {
        *minp = d;
      }
    }
  }
  return d;
}

/** For 16 bit images, this returns the nearest value to (k,l).
 ** 
 ** @param badPixels     Our bad pixel map
 **
 ** @param buf           Buffer contianing our image
 **
 ** @param bufWidth      image width
 ** 
 ** @param bufHeight     image height
 **
 ** @param k             index along height around which to find the max
 **
 ** @param l             index along width around which to find the max
 **
 ** @param yal           Dummy
 **
 ** @param yau           Dummy
 **
 ** @param xal           Dummy
 **
 ** @param xau           Dummy
 **
 ** @returns nearest value.  Bad pixels return 0
 */
uint32_t nearest16( uint32_t *badPixels, uint32_t *minp, int *nsatp, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  static const char *id = FILEID "nearest16";
  uint16_t *bp = (uint16_t *)buf;
  uint32_t rtn;
  int index;

  (void)id;

  index = (int)(k+0.5)*bufWidth + (int)(l+0.5);
  if (badPixels && *(badPixels + index)) {
    rtn = 0;
  } else {
    rtn = *(bp + index);
    if (rtn == 0xffff) {
      (*nsatp)++;
    }
  }

  if (*minp > rtn) {
    *minp = rtn;
  }
  return rtn;
}

/** For 32 bit images, this returns the nearest value to (k,l).
 ** 
 ** @param badPixels     Our bad pixel map
 **
 ** @param buf           Buffer contianing our image
 **
 ** @param bufWidth      image width
 ** 
 ** @param bufHeight     image height
 **
 ** @param k             index along height around which to find the max
 **
 ** @param l             index along width around which to find the max
 **
 ** @param yal           Dummy
 **
 ** @param yau           Dummy
 **
 ** @param xal           Dummy
 **
 ** @param xau           Dummy
 **
 ** @returns nearest value.  Bad pixels return 0
 */
uint32_t nearest32( uint32_t *badPixels, uint32_t *minp, int *nsatp, void *buf, int bufWidth, int bufHeight, double k, double l, int yal, int yau, int xal, int xau) {
  static const char *id = FILEID "nearest32";
  int index;
  uint32_t *bp = (uint32_t *)buf;
  uint32_t rtn;

  (void)id;

  index = (int)(k+0.5)*bufWidth + (int)(l+0.5);
  if (badPixels && *(badPixels + index)) {
    rtn = 0;
  } else {
    rtn = *(bp + index);
    if (rtn == 0xffffffff) {
      (*nsatp)++;
    }
  }

  if (*minp > rtn) {
    *minp = rtn;
  }

  return rtn;
}

/** Reduce the given 16 bit image
 **
 ** @param  src       Full sized source image
 **
 ** @param  dst       Reduced destination image
 **
 ** @param  x         Left edge on source image
 **
 ** @param  y         Top of source image
 **
 ** @param  winWidth  Width of portion of the source we want to look at
 **
 ** @param  winHeight Height of the portion of the source we want to look at
 */
void reduceImage16( isImageBufType *src, isImageBufType *dst, int x, int y, int winWidth, int winHeight) {
  static const char *id = FILEID "reduceImage16";

  uint32_t (*cvtFunc)(uint32_t *, uint32_t *, int *, void *, int, int, double, double, int, int, int, int);

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
  int nsat;
  uint32_t min; 
  int spots;
  int bin;
  int ice_spots;

  (void)id;

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

  nsat = 0;

  for (row=0; row<dstHeight; row++) {
    // "index" of vertical position on original image
    d_row = row * winHeight/(double)(dstHeight) + y;

    for (col=0; col<dstWidth; col++) {
      // "index" of the horizontal position on the original image
      d_col = col * winWidth/(double)(dstWidth) + x;

      if (d_row < 0 || d_row >= src->buf_height || d_col < 0 || d_col >= src->buf_width) {
        pxl = 0;
      } else {
        pxl = cvtFunc( src->bad_pixel_map, &min, &nsat, srcBuf, srcWidth, srcHeight, d_row, d_col, yal, yau, xal, xau);
      }

      if (pxl != 0xffffffff) {
        add_to_stats(dst, row, col, pxl);
      }
      *(dstBuf + row*dstWidth + col) = pxl;
    }
  }

  calc_stats(dst);

  if (json_integer_value(json_object_get(src->meta,"n")) <= json_integer_value(json_object_get(dst->meta, "n"))) {
    set_json_object_integer(id, src->meta, "n",          json_integer_value(json_object_get(dst->meta, "n")));
    set_json_object_real(id,    src->meta, "mean",       json_real_value(json_object_get(dst->meta, "mean")));
    set_json_object_real(id,    src->meta, "rms",        json_real_value(json_object_get(dst->meta, "rms")));
    set_json_object_real(id,    src->meta, "stddev",     json_real_value(json_object_get(dst->meta, "stddev")));
    set_json_object_integer(id, src->meta, "min",        json_integer_value(json_object_get(dst->meta, "min")));
    set_json_object_integer(id, src->meta, "max",        json_integer_value(json_object_get(dst->meta, "max")));
    set_json_object_integer(id, src->meta, "nSaturated", nsat);
  }

  // Count the spots
  spots = 0;
  ice_spots = 0;

  for (row=0; row < dstHeight; row++) {
    for (col=0; col<dstWidth; col++) {
      pxl = *(dstBuf + row*dstWidth + col);
      
      bin = get_bin_number(dst, col, row);
      
      if ((pxl - dst->bins[bin].mean) > (IS_SPOT_SENSITIVITY * dst->bins[bin].rms)) {
        if (bin < IS_OUTPUT_IMAGE_BINS) {
          spots++;
        } else {
          ice_spots++;
        }
      }
    }
  }
  if (dstHeight > 128) {
    fprintf(stdout, "%s: spots: %d   n: %d  mean: %f  rms: %f  stddev: %f\n",
            id, spots,
            (int)json_integer_value(json_object_get(dst->meta, "n")),
            (double)json_real_value(json_object_get(dst->meta, "mean")),
            (double)json_real_value(json_object_get(dst->meta, "rms")),
            (double)json_real_value(json_object_get(dst->meta, "stddev")));
  }

  set_json_object_integer(id, src->meta, "spots", spots);
}

/** Reduce the given 32 bit image
 **
 ** @param  src       Full sized source image
 **
 ** @param  dst       Reduced destination image
 **
 ** @param  x         Left edge on source image
 **
 ** @param  y         Top of source image
 **
 ** @param  winWidth  Width of portion of the source we want to look at
 **
 ** @param  winHeight Height of the portion of the source we want to look at
 */
void reduceImage32( isImageBufType *src, isImageBufType *dst, int x, int y, int winWidth, int winHeight) {
  static const char *id = FILEID "reduceImage32";
  uint32_t (*cvtFunc)(uint32_t *, uint32_t *, int *, void *, int, int, double, double, int, int, int, int);

  int row=0, col=0;
  int xa, ya;
  int xal, yal, xau, yau;
  uint32_t pxl;
  double d_row, d_col;
  uint32_t *srcBuf;
  uint32_t *dstBuf;
  int dstWidth;
  int dstHeight;
  int srcWidth;
  int srcHeight;
  int nsat;
  uint32_t min;
  int spots;
  int bin;
  int ice_spots;

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
    cvtFunc = nearest32;
  } else {
    cvtFunc = maxBox32;
  }

  nsat = 0;
  min  = 0xffffffff;
  for (row=0; row<dstHeight; row++) {
    // "index" of vertical position on original image
    d_row = row * (double)winHeight/(double)(dstHeight) + y;

    for (col=0; col<dstWidth; col++) {
      // "index" of the horizontal position on the original image
      d_col = col * (double)winWidth/(double)(dstWidth) + x;

      pxl = cvtFunc( src->bad_pixel_map, &min, &nsat, srcBuf, srcWidth, srcHeight, d_row, d_col, yal, yau, xal, xau);

      if (pxl != 0xffffffff) {
        add_to_stats(dst, row, col, pxl);
      }

      *(dstBuf + row*dstWidth + col) = pxl;
    }
  }
  
  calc_stats(dst);

  if (json_integer_value(json_object_get(src->meta,"n")) <= json_integer_value(json_object_get(dst->meta, "n"))) {
    set_json_object_integer(id, src->meta, "n",          json_integer_value(json_object_get(dst->meta, "n")));
    set_json_object_real(id,    src->meta, "mean",       json_real_value(json_object_get(dst->meta, "mean")));
    set_json_object_real(id,    src->meta, "rms",        json_real_value(json_object_get(dst->meta, "rms")));
    set_json_object_real(id,    src->meta, "stddev",     json_real_value(json_object_get(dst->meta, "stddev")));
    set_json_object_integer(id, src->meta, "min",        json_integer_value(json_object_get(dst->meta, "min")));
    set_json_object_integer(id, src->meta, "max",        json_integer_value(json_object_get(dst->meta, "max")));
    set_json_object_integer(id, src->meta, "nSaturated", nsat);
  }

  // Now for the spot counter
  spots = 0;
  ice_spots = 0;

  for (row=0; row < dstHeight; row++) {
    for (col=0; col<dstWidth; col++) {
      pxl = *(dstBuf + row*dstWidth + col);
      
      bin = get_bin_number(dst, col, row);
      
      if ((pxl - dst->bins[bin].mean) > (IS_SPOT_SENSITIVITY * dst->bins[bin].rms)) {
        if (bin < IS_OUTPUT_IMAGE_BINS) {
          spots++;
        } else {
          ice_spots++;
        }
      }
    }
  }

  if (dstHeight > 128) {
    fprintf(stdout, "%s: spots: %d   n: %d  mean: %f  rms: %f  stddev: %f\n",
            id, spots,
            (int)json_integer_value(json_object_get(dst->meta, "n")),
            (double)json_real_value(json_object_get(dst->meta, "mean")),
            (double)json_real_value(json_object_get(dst->meta, "rms")),
            (double)json_real_value(json_object_get(dst->meta, "stddev")));
  }

  set_json_object_integer(id, src->meta, "spots", spots);
}

            
/** Image reduction is defined by a "zoom" and a "sector".
 **
 **  The width and height of the original image are divided by "zoom"
 **  and the resulting segments addressed by column and row indices
 **  starting with the upper left hand corner.  For example:
 **
 **  Zoom: 4 gives 16 segments from [0,0] to [3,3].  Zoom: 1.5 gives 4
 **  segments from [0,0] to [1,1] where the right half of [0,1] is
 **  blank, the bottom half of [0,1] is blank, and only the upper left
 **  hand quadrant of [1,1] is potentially non-blank.
 **
 **
 **  Call with
 **
 **    @param wctx        Our worker contex:
 **      @li @c wctx->ctxMutex  Keep our parallel worlds from colliding
 **
 **    @param rc          Open redis context to local redis server
 **
 **    @param job         Request from user.  We use the following properties here
 **      @li @c job->fn     File name of the data we are interested in
 **      @li @c job->frame  Requested frame.  Default is 1
 **      @li @c job->zoom   Ratio of full source image to the portion of the source image we are processing. Zoom will be rounded to the nearest 0.1
 **      @li @c job->segcol See above for discussion of col/row/zoom
 **      @li @c job->segrow See above for discussion of col/row/zoom
 **      @li @c job->xsize  Width of output image in pixels
 **      @li @c job->ysize  Height of output image in pixels
 **
 **
 **  Return with
 **
 **    read locked buffer
 */
isImageBufType *isReduceImage(isWorkerContext_t *wctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isReducedImage";
  isImageBufType *rtn;
  isImageBufType *raw;
  double zoom;
  double segcol;
  double segrow;
  //  double seglen;
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
  int winWidth;                                                         // width of input image to map to output image
  int winHeight;                                                        // height of input image to map to output image
  int dstWidth  = json_integer_value(json_object_get(job, "xsize"));    // width, in pixels, of output image
  int dstHeight;                                                        // height, in pixels, calculated once we know the source image dimensions

  fn    = json_string_value(json_object_get(job, "fn"));
  frame = json_integer_value(json_object_get(job, "frame"));

  zoom   = json_number_value(json_object_get(job, "zoom"));
  segcol = json_number_value(json_object_get(job, "segcol"));
  segrow = json_number_value(json_object_get(job, "segrow"));
  
  //
  // Reality check on zoom
  //
  zoom = (floor(10.0*zoom+0.5))/10.0;   // round to the nearest 0.1
  if (zoom <= 1.0) {
    zoom = 1.0;
    segcol = 0.;
    segrow = 0.;
  }
  
  //
  // Reality check on segcol and segrow
  //
  /*
  seglen = ceil(zoom);
  segcol = segcol > seglen - 1. ? seglen - 1. : segcol;
  segrow = segrow > seglen - 1. ? seglen - 1. : segrow;
  */

  //
  // Refuse really tiny conversions
  dstWidth  = dstWidth  < 8 ? 8 : dstWidth;

  //
  // Reality check on the destination image size to keep weirdos from
  // finding buffer overflows.
  //
  if (dstWidth > 10000) {
    isLogging_err("%s: unlikely valid destination size width %d\n", id, dstWidth);
    return NULL;
  }

  if (fn == NULL) {
    isLogging_err("%s: Cannot find file name in job\n", id);
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
    isLogging_err("%s: Unlikely gid %d\n", id, gid);
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
    isLogging_crit("%s: Out of memory (reducedKey)\n", id);
    exit (-1);
  }
  snprintf(reducedKey, reducedKeyStrlen, "%d:%s-%d-%0.1f-%0.3f-%0.3f-%d",
           getegid(), fn, frame, zoom, segcol, segrow, dstWidth);
  reducedKey[reducedKeyStrlen] = 0;
 
  rtn = isGetImageBufFromKey(wctx, rc, reducedKey);

  if (rtn == NULL || rtn->buf != NULL) {
    //
    // We either failed completely or succeeded without really trying.
    // Either way we are done here.  When rtn is not null the buffer
    // is read locked and in_use incremented.  Don't forget to release it.
    //
    free(reducedKey);
    return rtn;
  }

  //
  // Here we have a write locked buffer (with in_use = 1) with nothing in it.
  //
  
  // Get the unreduced file
  raw = isGetRawImageBuf(wctx, rc, job);
  if (raw == NULL) {
    isLogging_err("%s: Failed to get raw data for %s\n", id, rtn->key);
    //
    // Can't fill the buffer we want, should probably raise some kind
    // of hell.  Presumably isGetRawImageBuf complained to the
    // authorities.
    //
    pthread_rwlock_unlock(&rtn->buflock);
    pthread_mutex_lock(&wctx->ctxMutex);
    rtn->in_use--;
    assert(rtn->in_use >= 0);
    pthread_mutex_unlock(&wctx->ctxMutex);

    free(reducedKey);
    return NULL;
  }
  
  // raw is the the raw data we'll be reducing.  rtn is the reduced
  // buffer we'll be filling.
  // 
  // Here raw is read locked and rtn is write locked.
  //
  srcWidth  = json_integer_value(json_object_get(raw->meta, "x_pixels_in_detector"));       // width, in pixels, of full input image
  srcHeight = json_integer_value(json_object_get(raw->meta, "y_pixels_in_detector"));       // height, in pixels, of full input image
  
  dstHeight = (double)srcHeight * (double)dstWidth / (double)srcHeight;

  image_depth = json_integer_value(json_object_get(raw->meta, "image_depth"));
  if (image_depth != 2 && image_depth != 4) {
    isLogging_err("%s: bad image depth %d.  Likely this is a serious error somewhere\n", id, image_depth);
    exit (-1);
  }

  winWidth  = srcWidth / zoom;
  winHeight = srcHeight / zoom;

  rtn->buf_size = dstWidth * dstHeight * image_depth;
  rtn->buf = calloc(1, rtn->buf_size);
  if (rtn->buf == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  rtn->buf_width  = dstWidth;
  rtn->buf_height = dstHeight;
  rtn->buf_depth  = image_depth;

  set_json_object_integer(id, raw->meta, "frame", frame);

  rtn->meta = json_copy(raw->meta);
  json_incref(rtn->meta);

  x = winWidth  * segcol;
  y = winHeight * segrow;

  set_up_bins(raw, rtn, winWidth, winHeight, x, y);

  switch (image_depth) {
  case 2:
    reduceImage16(raw, rtn, x, y, winWidth, winHeight);
    break;

  case 4:
    reduceImage32(raw, rtn, x, y, winWidth, winHeight);
    break;

  default:
    isLogging_err("%s: Unusable image depth %d\n", id, image_depth);
    exit (-1);
  }

  pthread_rwlock_unlock(&raw->buflock);

  // We don't need the raw buffer anymore  pthread_mutex_lock(&wctx->ctxMutex);
  raw->in_use--;
  assert(raw->in_use >= 0);
  pthread_mutex_unlock(&wctx->ctxMutex);

  //
  // Exchange our write lock for a read lock to let our other threads get to work.
  //
  pthread_rwlock_unlock(&rtn->buflock);
  pthread_rwlock_rdlock(&rtn->buflock);
  //
  // Let the other processes get started on this one too.
  //
  //isWriteImageBufToRedis(rtn, rc);

  free(reducedKey);
  return rtn;
}  
