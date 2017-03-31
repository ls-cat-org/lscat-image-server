#include "is.h"

void isJpegLabel(const char *label, int width, int height, struct jpeg_compress_struct *cinfop) {
  static const char *id = FILEID "isJpegLabel";
  const isBitmapFontType *bmp;          // Our chosen bitmap font
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
  uint8_t *red, *blue, *green;
  JSAMPROW row_buffer;
  int i;
  int ci;

  row_buffer = calloc(width, sizeof(*row_buffer) * 3);
  if (row_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (row_buffer)\n", id);
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
  label_ymax = height > bmp->height ? bmp->height : height;
  label_xmax = width;
    
  // i loops over scan lines
  // cy loops over character scan lines
  // ci loops over character columns
  //
    
  for (cy=0, i=label_ymin; i<=label_ymax; cy++, i++) {
    red   = row_buffer;
    green = row_buffer + 1;
    blue  = row_buffer + 2;
    
    memset( row_buffer, 0xff, 3 * width);
    
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
    jpeg_write_scanlines(cinfop, &row_buffer, 1);
  }
  free(row_buffer);
}


void isJpegSend(json_t *job, json_t *meta, JOCTET *out_buffer, int jpeg_len) {
  static const char *id = FILEID "isJpegSend";

  /**
   *  We are going to publish this resulting jpeg to redis along with
   *  the job and meta data objects.  We'll use the redis
   *  request-response protocol to serialize the objects and image.
   *  See https://redis.io/topics/protocol for details. We could embed
   *  any serialization protocol we want to form our message but this
   *  choice is advantageous for the following reasons:
   *
   *    1) It's well documented and complete.
   *
   *    2) It's easy to generate and easy to parse.
   *
   *    3) If the redis pub/sub model does not work out for us we have
   *       other options including dumping the message directly to a
   *       redis key or to an awaiting socket ala our version 1 image
   *       server.
   */
  char *job_str;
  char *meta_str;
  redisContext *rcRemote;
    
  job_str = NULL;
  if (job != NULL) {
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  }

  meta_str = NULL;
  if (meta != NULL) {
    meta_str = json_dumps(meta, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  }
    
  rcRemote = redisConnect(json_string_value(json_object_get(job, "rtn_addr")), json_integer_value(json_object_get(job, "rtn_port")));
  if (rcRemote == NULL || rcRemote->err) {
    if (rcRemote) {
      fprintf(stderr, "%s: Failed to connect to redis %s:%d. Error: %s\n", id, json_string_value(json_object_get(job,"rtn_addr")), (int)json_integer_value(json_object_get(job, "rtn_port")), rcRemote->errstr);
    } else {
      fprintf(stderr, "%s: Failed to get redis context\n", id);
    }
    fflush(stderr);
    exit (-1);
  }
  
  //                             pub       jlen  job  mlen meta  jlen  jpeg
  redisCommand(rcRemote, "PUBLISH %s *3\r\n$%d\r\n%s%s$%d\r\n%s%s$%d\r\n%b%s",
               json_string_value(json_object_get(job, "publisher")),
               job_str  == NULL ? -1 : strlen(job_str),
               job_str  == NULL ? "" : job_str,
               job_str  == NULL ? "" : "\r\n",
               meta_str == NULL ? -1 : strlen(meta_str),
               meta_str == NULL ? "" : meta_str,
               meta_str == NULL ? "" : "\r\n",
               jpeg_len == 0    ? -1 : jpeg_len,
               jpeg_len == 0    ? (JOCTET *)"" : out_buffer,
               jpeg_len,
               jpeg_len == 0    ? "" : "\r\n"
               );
  redisFree(rcRemote);
}


void isJpegBlank(json_t *job) {
  static const char *id = FILEID "isJpegBlank";
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  struct jpeg_destination_mgr dmgr;
  JSAMPROW row_buffer;
  JOCTET *out_buffer;
  int labelHeight;
  int col;
  int row;
  int height;
  int width;
  uint8_t *red, *blue, *green;
  const char *label;
    
  width = json_integer_value(json_object_get(job, "xsize"));
  width = width < 8 ? 8 : width;
  height = width;

  labelHeight = 0;
  label = json_string_value(json_object_get(job, "label"));
  if (label != NULL && *label) {
    labelHeight = json_integer_value(json_object_get(job, "labelHeight"));
    labelHeight = labelHeight < 0  ?  0 : labelHeight;    // labels can't have negative height
    labelHeight = labelHeight > 64 ?  0 : labelHeight;    // ignore requests for really big labels
  }

  row_buffer = calloc(width, sizeof(*row_buffer) * 3);
  if (row_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (row_buffer)\n", id);
    exit (-1);
  }

  out_buffer = calloc(width * height, (sizeof(*out_buffer) + labelHeight) * 3);
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
  dmgr.free_in_buffer      = (width * height + labelHeight) * 3;

  cinfo.dest = &dmgr;
  cinfo.image_width  = width;
  cinfo.image_height = height + labelHeight;
  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */
  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 95, TRUE);
  jpeg_start_compress(&cinfo, TRUE);

  if (labelHeight) {
    isJpegLabel(label, width, labelHeight, &cinfo);
  }

  for (row=0; row<height; row++) {
    red   = row_buffer;
    green = row_buffer + 1;
    blue  = row_buffer + 2;

    for (col=0; col<width; col++) {
      *red   = 0xf0;
      *green = 0xf0;
      *blue  = 0xf0;

      red   += 3;
      green += 3;
      blue  += 3;
    }
    jpeg_write_scanlines(&cinfo, &row_buffer, 1);
  }
  jpeg_finish_compress(&cinfo);

  isJpegSend(job, NULL, out_buffer, (int)(cinfo.dest->next_output_byte - out_buffer));
  free(row_buffer);
  free(out_buffer);
  return;
}

void isJpeg( isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isJpeg";
  const char *fn;                       // file name from job.
  isImageBufType *imb;
  JSAMPROW row_buffer;
  JOCTET *out_buffer;
  int labelHeight;
  int row, col;
  uint16_t *bp16, v16;
  uint32_t *bp32, v32;
  uint8_t *red, *blue, *green;
  int32_t wval, bval;
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  struct jpeg_destination_mgr dmgr;
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

  //fprintf(stdout, "%s: label: %s  labelHeight: %d\n", id, json_string_value(json_object_get(job,"label")), labelHeight);

  if (labelHeight && json_string_value(json_object_get(job,"label"))) {
    snprintf(label, sizeof(label)-1, "%s %d", json_string_value(json_object_get(job,"label")), (int)json_integer_value(json_object_get(job,"frame")));
    label[sizeof(label)-1] = 0;

    isJpegLabel(label, imb->buf_width, labelHeight, &cinfo);
  }

  wval = json_integer_value(json_object_get(job,"wval"));
  bval = json_integer_value(json_object_get(job, "contrast"));
  
  //
  // Perhaps autoscale black values
  //
  if (bval <= 0) {
    bval = json_real_value(json_object_get(imb->meta, "mean")) + json_real_value(json_object_get(imb->meta, "stddev"));
  }
  
  //
  // Perhaps autoscale white values
  //
  if (wval < 0) {
    wval = json_real_value(json_object_get(imb->meta, "mean")) - json_real_value(json_object_get(imb->meta, "stddev"));
  }

  wval = wval < 0 ? 0 : wval;
  bval = bval <= wval ? wval+1 : bval;  

  fprintf(stdout, "%s: wval=%d  bval=%d\n", id, wval, bval);

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
              *red = *green = *blue = 255.0 - (double)(v16 - wval)/(double)(bval - wval) * 255.0;
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
              *red = *green = *blue = 255.0 - (double)(v32 - wval)/(double)(bval - wval) * 255.0;
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

  //fprintf(stdout, "%s: jpeg size = %d for image %s\n", id, (int)(cinfo.dest->next_output_byte - out_buffer), imb->key);

  isJpegSend(job, imb->meta, out_buffer, (int)(cinfo.dest->next_output_byte - out_buffer));

  free(row_buffer);
  free(out_buffer);

  pthread_rwlock_unlock(&imb->buflock);

  pthread_mutex_lock(&ibctx->ctxMutex);
  imb->in_use--;
  pthread_mutex_unlock(&ibctx->ctxMutex);
  return;
}
