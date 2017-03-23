#include "is.h"

void isJpegBlank(json_t *job) {
  static const char *id = FILEID "isJpegBlank";

}

void isJpeg( isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isJpeg";
  const char *fn;                       // file name from job.
  const isBitmapFontType *bmp;          // Our chosen bitmap font
  isImageBufType *imb;
  unsigned char  *bufo;
  unsigned char  *bufo_with_label;
  int labelHeight;
  int row, col;
  uint16_t *bp16, v16;
  uint32_t *bp32, v32;
  uint8_t *red, *blue, *green;
  uint32_t wval, bval;
  int mask;                     // used to find bit in font
  int sbc;                      // the "sub" byte in the font needed for font width > 8
  int bpc;                      // bytes per character.  ie, 1 for 6x13, 2 for 9x15
  int bmc;                      // the current byte in the font
  int cy;                       // current scan line within the bitmap font
  int label_ymin;               // start scan line for the top of the text
  int label_ymax;               // bottom scan line for, well, the bottom of the text
  int label_xmax;               // RHS of text
  int ib;                       // index in bitmap of current char
  int ix;                       // x position in scan line of current char

  fn = json_string_value(json_object_get(job, "fn"));
  if (fn == NULL || strlen(fn) == 0) {
    isJpegBlank(job);
    return;
  }

  // when isReduceImage returns a buffer it is read locked
  imb = isReduceImage(ibctx, rc, job);
  if (imb == NULL) {
    char *tmps;

    tmps = json_dumps(job, JSON_SORT_KEYS | JSON_COMPACT | JSON_INDENT(0));
    fprintf(stderr, "%s: missing data for job %s\n", id, tmps);

    free(tmps);
    return;
  }

  labelHeight = json_integer_value(json_object_get(job, "labelheight"));
  labelHeight = labelHeight < 0  ?  0 : labelHeight;    // labels can't have negative height
  labelHeight = labelHeight > 64 ?  0 : labelHeight;    // ignore requests for really big labels

  bufo_with_label = malloc((imb->buf_width * imb->buf_height + labelHeight) * 3);
  if (bufo == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  // Write the label
  //
  bmp = &isBitmapFontBitmaps[3];
  bpc = bmp->width / 8 + 1;             // bytes per character in this font

  //
  // Placement of the label within the banner.  At this point we only
  // support left justified text.  TODO: truncate the beginning of the
  // string if the label is too long to fit since it's the end of the
  // string that distingushes one frame from another.
  //
  label_ymin = 0;
  label_ymax = labelHeight > bmp->height ? bmp->height : labelHeight;
  label_xmax = imb->buf_width;

  bufo  = bufo_with_label;
  red   = bufo;
  green = bufo + 1;
  blue  = bufo + 2;

  for (cy=0, i=label_ymin; i<=label_ymax; cy++, i++) {
    // Loop over characters
    for (ci=0; label[ci] != 0; ci++) {
      if (label[ci] < 32) {
        continue;
      }
      if (ci * bmp->width >= label_xmax) {
        // No room on line for this character
        break;
      }
      sbc = 0;
      bmc = bmp->bitmap[(bmp->height*(label[ci] - 32) + cy) * bpc];
      mask = 0x80;
      for (ib=0; ib<bmp->width; ib++) {
        ix = ci * bmp->width + ib;
        if (ix > label_xmax) {
          break;
        }

        bp = bufo + 3*i*(is->xsize) + 3*ix;
        if (mask & bmc) {
          *red = *green = *blue = 255;
        }

        mask >>= 1;
        if (!mask) {
          mask = 0x80;
          sbc++;
          bmc = bmp->bitmap[(bmp->height*(label[ci] - 32) + cy) * bpc + sbc];
        }
        red   += 3;
        green += 3;
        blue  += 3;
      }
    }
  }



  // TODO: stick in the autoscale
  //
  wval = json_integer_value(json_object_get(job,"wval"));
  bval = json_integer_value(json_object_get(job, "contrast"));
  
  wval = wval < 0 ? 0 : wval;
  bval = bval <= wval ? wval+1 : bval;  

  bufo  = bufo_with_label + 3 * labelHeight;
  red   = bufo;
  green = bufo + 1;
  blue  = bufo + 2;
  if (imp->buf_depth == 4) {
    bp16 = imp->buf;
    for (row=0; row<imb->buf_height; row++) {
      for (col=0; col<imb->buf_width; col++) {
        v16 = *(bp16 + imp->buf_width * row + col);
        if (v16 == 0xffff) {
          *red   = 0xff;
          *green = 0;
          *blue  = 0;
        } else {
          if (v16 <= wval) {
            *red = *green = *blue = 0;
          } else {
            if (v16 >= bval) {
              *red = *green = *blue = 0xff;
            } else {
              *red = *green = *blue = (v16 - wval)/(bval - wval) * 0xff;
            }
          }
        }
        red   += 3;
        green += 3;
        blue  += 3;
      }
    }
  } else {
    bp32 = imp->buf;
    for (row=0; row<imb->buf_height; row++) {
      for (col=0; col<imb->buf_width; col++) {
        v32 = *(bp32 + imp->buf_width * row + col);
        if (v16 == 0xffffffff) {
          *red   = 0xff;
          *green = 0;
          *blue  = 0;
        } else {
          if (v32 <= wval) {
            *red = *green = *blue = 0;
          } else {
            if (v32 >= bval) {
              *red = *green = *blue = 0xff;
            } else {
              *red = *green = *blue = (v32 - wval)/(bval - wval) * 0xff;
            }
          }
        }
        red   += 3;
        green += 3;
        blue  += 3;
      }
    }
  }

  bufo_with_label = NULL;


  



  pthread_rwlock_unlock(&imb->buflock);

  pthread_mutex_lock(&ibctx->ctxMutex);
  imb->in_use--;
  pthread_mutex_unlock(&ibctx->ctxMutex);
  return;
}
