#include "is.h"

void isJpegBlank(json_t *job) {
  static const char *id = FILEID "isJpegBlank";
  (void)id;

}

void isJpeg( isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isJpeg";
  const char *fn;                       // file name from job.
  const isBitmapFontType *bmp;          // Our chosen bitmap font
  isImageBufType *imb;
  JSAMPROW row_buffer;
  JOCTET *out_buffer;
  int labelHeight;
  int row, col;
  uint16_t *bp16, v16;
  uint32_t *bp32, v32;
  uint8_t *red, *blue, *green;
  int32_t wval, bval;
  uint16_t mask;                // used to find bit in font
  int sbc;                      // the "sub" byte in the font needed for font width > 8
  int bpc;                      // bytes per character.  ie, 1 for 6x13, 2 for 9x15
  int bmc;                      // the current byte in the font
  int cy;                       // current scan line within the bitmap font
  int label_ymin;               // start scan line for the top of the text
  int label_ymax;               // bottom scan line for, well, the bottom of the text
  int label_xmax;               // RHS of text
  int ib;                       // index in bitmap of current char
  int ix;                       // x position in scan line of current char
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  struct jpeg_destination_mgr dmgr;
  int i;
  int ci;
  char label[64];

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

  labelHeight = json_integer_value(json_object_get(job, "labelHeight"));
  labelHeight = labelHeight < 0  ?  0 : labelHeight;    // labels can't have negative height
  labelHeight = labelHeight > 64 ?  0 : labelHeight;    // ignore requests for really big labels

  row_buffer = calloc(imb->buf_width, sizeof(*row_buffer) * 3);
  if (row_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (row_buffer)\n", id);
    exit (-1);
  }

  out_buffer = calloc(imb->buf_width * imb->buf_height, (sizeof(*out_buffer) + labelHeight) * 3);
  if (out_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (out_buffer)\n", id);
    exit (-1);
  }

  //
  // Setup the jpeg stuff
  //
  cinfoSetup = 0;

  if( setjmp( j_jumpHere)) {
    if( cinfoSetup) {
      jpeg_destroy_compress( &cinfo);
    }
    free(row_buffer);
    free(out_buffer);

    pthread_rwlock_unlock(&imb->buflock);

    pthread_mutex_lock(&ibctx->ctxMutex);
    imb->in_use--;
    pthread_mutex_unlock(&ibctx->ctxMutex);
    return;
  }

  void jerror_handler( j_common_ptr cp) {
    fprintf( stderr, "%s: jpeg compression error\n", id);
    longjmp( *(jmp_buf *)cp->client_data, 1);
  }

  void init_buffer(struct jpeg_compress_struct* cinfo) {}
  int empty_buffer(struct jpeg_compress_struct* cinfo) {
    return 1;
  }
  void term_buffer(struct jpeg_compress_struct* cinfo) {}

  cinfo.err = jpeg_std_error(&jerr);
  cinfo.err->error_exit = jerror_handler;
  cinfo.client_data    = &j_jumpHere;
  jpeg_create_compress(&cinfo);
  cinfoSetup = 1;

  
  dmgr.init_destination    = init_buffer;
  dmgr.empty_output_buffer = empty_buffer;
  dmgr.term_destination    = term_buffer;
  dmgr.next_output_byte    = out_buffer;
  dmgr.free_in_buffer      = (imb->buf_width * imb->buf_height + labelHeight) * 3;

  cinfo.dest = &dmgr;

  cinfo.image_width  = imb->buf_width;
  cinfo.image_height = imb->buf_height + labelHeight;

  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */

  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 95, TRUE);

  jpeg_start_compress(&cinfo, TRUE);

  //
  // TODO: Rayonix images already have the frame number in the label,
  // so don't add it for these.  How to tell?  Probably imb should
  // have a field for the detector type and/or the number of frames in
  // the file.  When the number is 1 then don't add the frame number
  // to the label.
  //

  fprintf(stderr, "%s: label: %s  labelHeight: %d\n", id, json_string_value(json_object_get(job,"label")), labelHeight);

  if (labelHeight && json_string_value(json_object_get(job,"label"))) {

    snprintf(label, sizeof(label)-1, "%s %d", json_string_value(json_object_get(job,"label")), (int)json_integer_value(json_object_get(job,"frame")));
    label[sizeof(label)-1] = 0;

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
    
    // i loops over scan lines
    // cy loops over character scan lines
    // ci loops over character columns
    //
    
    for (cy=0, i=label_ymin; i<=label_ymax; cy++, i++) {
      red   = row_buffer;
      green = row_buffer + 1;
      blue  = row_buffer + 2;
      
      memset( row_buffer, 0xff, 3 * imb->buf_width);

      for (ci=0; label[ci] != 0; ci++) {
        if (label[ci] < 32) {
          // Ignore control characters
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
          if (mask & bmc) {
            *red = *green = *blue = 0;
          } else {
            *red = *green = *blue = 0xff;
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
      jpeg_write_scanlines(&cinfo, &row_buffer, 1);
    }
  }


  // TODO: stick in the autoscale
  //
  wval = json_integer_value(json_object_get(job,"wval"));
  bval = json_integer_value(json_object_get(job, "contrast"));
  
  wval = wval < 0 ? 0 : wval;
  bval = bval <= wval ? wval+1 : bval;  

  fprintf(stderr, "%s: wval=%d  bval=%d\n", id, wval, bval);

  if (imb->buf_depth == 2) {
    bp16 = imb->buf;
    for (row=0; row<imb->buf_height; row++) {
      red   = row_buffer;
      green = row_buffer + 1;
      blue  = row_buffer + 2;
      for (col=0; col<imb->buf_width; col++) {
        v16 = *(bp16 + imb->buf_width * row + col);
        if (v16 == 0xffff) {
          *red   = 0xff;
          *green = 0;
          *blue  = 0;
        } else {
          if (v16 <= wval) {
            *red = *green = *blue = 0xff;
          } else {
            if (v16 >= bval) {
              *red = *green = *blue = 0;
            } else {
              *red = *green = *blue = 0xff - (v16 - wval)/(bval - wval) * 0xff;
            }
          }
        }
        red   += 3;
        green += 3;
        blue  += 3;
      }
      jpeg_write_scanlines(&cinfo, &row_buffer, 1);
    }
  } else {
    bp32 = imb->buf;
    for (row=0; row<imb->buf_height; row++) {
      red   = row_buffer;
      green = row_buffer + 1;
      blue  = row_buffer + 2;
      for (col=0; col<imb->buf_width; col++) {
        v32 = *(bp32 + imb->buf_width * row + col);
        if (v32 == 0xffffffff) {
          *red   = 0xff;
          *green = 0;
          *blue  = 0;
        } else {
          if (v32 <= wval) {
            *red = *green = *blue = 0xff;
          } else {
            if (v32 >= bval) {
              *red = *green = *blue = 0;
            } else {
              *red = *green = *blue = 0xff - (v32 - wval)/(bval - wval) * 0xff;
            }
          }
        }
        red   += 3;
        green += 3;
        blue  += 3;
      }
      jpeg_write_scanlines(&cinfo, &row_buffer, 1);
    }
  }

  jpeg_finish_compress(&cinfo);

  fprintf(stderr, "%s: jpeg size = %d for image %s\n", id, (int)(cinfo.dest->next_output_byte - out_buffer), imb->key);

  {
    FILE *fp;
    int lngth = cinfo.dest->next_output_byte - out_buffer;
    fp = fopen("/tmp/a.jpeg", "w");
    fwrite(out_buffer, lngth, 1, fp);
    fclose(fp);
  }


  free(row_buffer);
  free(out_buffer);

  pthread_rwlock_unlock(&imb->buflock);

  pthread_mutex_lock(&ibctx->ctxMutex);
  imb->in_use--;
  pthread_mutex_unlock(&ibctx->ctxMutex);
  return;
}
