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
  unsigned char *red, *blue, *green;
  unsigned char *row_buffer;
  int i;
  int ci;
  size_t row_buffer_size;

  row_buffer_size = width * sizeof(*row_buffer) * 3;

  row_buffer = calloc(1, row_buffer_size);
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
    
    memset( row_buffer, 0xff, row_buffer_size);
    
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

void zfree(void *data, void *hint) {
  if (data != NULL) {
    free(data);
  }
}

void isJpegSend(json_t *job, json_t *meta, unsigned char *out_buffer, int jpeg_len) {
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
  void *zctx;
  void *zsock;
  int err;
  zmq_msg_t zmsg;
  const char *tmp;
  char *publisher;

  tmp = json_string_value(json_object_get(job, "publisher"));
  if (tmp == NULL) {
    publisher = strdup("");
  } else {
    publisher = strdup(tmp);
  }
  if (publisher == NULL) {
    fprintf(stderr, "%s: Out of memory (publisher)\n", id);
    exit(-1);
  }

  errno = 0;
  zctx = zmq_ctx_new();
  if (zctx == NULL) {
    fprintf(stderr, "%s: Oddly, zmq_ctx_new failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }
  
  errno = 0;
  zsock = zmq_socket(zctx, ZMQ_REQ);
  if (zsock == NULL) {
    fprintf(stderr, "%s: Could not create zmq socket: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  {
    int opt;
    opt = 1;
    err = zmq_setsockopt(zsock, ZMQ_SNDTIMEO, &opt, sizeof(opt));
    if (err != 0) {
      fprintf(stderr, "%s: zmq_connect failed: %s\n", id, strerror(errno));
      exit (-1);
    }
  }    

  err = zmq_connect(zsock, json_string_value(json_object_get(job, "endpoint")));
  if (err != 0) {
    fprintf(stderr, "%s: zmq_connect failed: %s\n", id, strerror(errno));
    exit (-1);
  }

  job_str = NULL;
  if (job != NULL) {
    job_str = json_dumps(job, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  }
  if (job_str == NULL) {
    job_str = strdup("");
  }

  meta_str = NULL;
  if (meta != NULL) {
    meta_str = json_dumps(meta, JSON_SORT_KEYS | JSON_INDENT(0) | JSON_COMPACT);
  }
  if (meta_str == NULL) {
    meta_str = strdup("");
  }
    
  err = zmq_msg_init_data(&zmsg, publisher, strlen(publisher), zfree, NULL);
  if (err != 0) {
    fprintf(stderr, "%s: zmq_msg_init failed (publisher): %s\n", id, zmq_strerror(errno));
    exit (-1);
  }
  
  err = zmq_msg_send(&zmsg, zsock, ZMQ_SNDMORE);
  if (err < 0) {
    fprintf(stderr, "%s: sending publisher failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_msg_init_data(&zmsg, job_str, strlen(job_str), zfree, NULL);
  if (err != 0) {
    fprintf(stderr, "%s: zmq_msg_init failed (job_str): %s\n", id, zmq_strerror(errno));
    exit (-1);
  }
  
  err = zmq_msg_send(&zmsg, zsock, ZMQ_SNDMORE);
  if (err < 0) {
    fprintf(stderr, "%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_msg_init_data(&zmsg, meta_str, strlen(meta_str), zfree, NULL);
  if (err != 0) {
    fprintf(stderr, "%s: zmq_msg_init failed (meta_str): %s\n", id, zmq_strerror(errno));
    exit (-1);
  }
  
  err = zmq_msg_send(&zmsg, zsock, ZMQ_SNDMORE);
  if (err < 0) {
    fprintf(stderr, "%s: sending job_str failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_msg_init_data(&zmsg, out_buffer, jpeg_len, zfree, NULL);
  if (err != 0) {
    fprintf(stderr, "%s: zmq_msg_init failed (jpeg): %s\n", id, zmq_strerror(errno));
    exit (-1);
  }
  
  err = zmq_msg_send(&zmsg, zsock, 0);
  if (err < 0) {
    fprintf(stderr, "%s: sending jpeg failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_close(zsock);
  if (err != 0) {
    fprintf(stderr, "%s: closing socket failed: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  errno = 0;
  do {
    err = zmq_ctx_term(zctx);
  } while (errno == EINTR);

  if (err != 0) {
    fprintf(stderr, "%s: failed to terminate zmq context: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  /*
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
  */
  /*
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
  */

}


void isJpegBlank(json_t *job) {
  static const char *id = FILEID "isJpegBlank";
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  struct jpeg_destination_mgr dmgr;
  unsigned char *row_buffer;
  unsigned char *out_buffer;
  int labelHeight;
  int col;
  int row;
  int height;
  int width;
  unsigned char *red, *blue, *green;
  const char *label;
  size_t row_buffer_size;
  size_t out_buffer_size;

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

  row_buffer_size = width * sizeof(*row_buffer) * 3;

  row_buffer = calloc(1, row_buffer_size);
  if (row_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (row_buffer)\n", id);
    exit (-1);
  }

  out_buffer_size = width * (height + labelHeight) * sizeof(*out_buffer) * 3;
  if (out_buffer_size < MIN_JPEG_BUFFER) {
    out_buffer_size = MIN_JPEG_BUFFER;
  }

  out_buffer = calloc(1, out_buffer_size);
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
  dmgr.free_in_buffer      = out_buffer_size;

  cinfo.dest = &dmgr;
  cinfo.image_width  = width;
  cinfo.image_height = height + labelHeight;
  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */
  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 100, TRUE);
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
  //
  //  out_buffer is freed by zmq whenever it is done with it
  //
  //  free(out_buffer);
  return;
}

void isJpeg( isImageBufContext_t *ibctx, redisContext *rc, json_t *job) {
  static const char *id = FILEID "isJpeg";
  const char *fn;                       // file name from job.
  isImageBufType *imb;
  unsigned char *row_buffer;
  unsigned char *out_buffer;
  int labelHeight;
  int row, col;
  uint16_t *bp16, v16;
  uint32_t *bp32, v32;
  unsigned char *red, *blue, *green;
  int32_t wval, bval;
  struct jpeg_compress_struct cinfo;
  int cinfoSetup;
  jmp_buf j_jumpHere;
  struct jpeg_error_mgr jerr;
  struct jpeg_destination_mgr dmgr;
  char label[64];
  size_t row_buffer_size;
  size_t out_buffer_size;

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

  row_buffer_size = imb->buf_width * sizeof(*row_buffer) * 3;

  row_buffer = calloc(1, row_buffer_size);
  if (row_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (row_buffer)\n", id);
    exit (-1);
  }

  out_buffer_size = imb->buf_width * (imb->buf_height + labelHeight) * sizeof(*out_buffer) * 3;
  if (out_buffer_size < MIN_JPEG_BUFFER) {
    out_buffer_size = MIN_JPEG_BUFFER;
  }

  out_buffer = calloc(1, out_buffer_size);
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

    assert(imb->in_use >= 0);

    pthread_mutex_unlock(&ibctx->ctxMutex);
    return;
  }

  void jerror_handler( j_common_ptr cp) {
    fprintf( stderr, "%s: jpeg compression error\n", id);
    longjmp( *(jmp_buf *)cp->client_data, 1);
  }

  void init_buffer(struct jpeg_compress_struct* cinfop) {
    cinfop->dest->next_output_byte = out_buffer;
    cinfop->dest->free_in_buffer   = out_buffer_size;
  }
  int empty_buffer(struct jpeg_compress_struct* cinfop) {
    return 1;
  }
  void term_buffer(struct jpeg_compress_struct* cinfop) {}

  cinfo.err = jpeg_std_error(&jerr);
  cinfo.err->error_exit = jerror_handler;
  cinfo.client_data    = &j_jumpHere;
  jpeg_create_compress(&cinfo);
  cinfoSetup = 1;

  
  dmgr.init_destination    = init_buffer;
  dmgr.empty_output_buffer = empty_buffer;
  dmgr.term_destination    = term_buffer;

  cinfo.dest = &dmgr;

  cinfo.image_width  = imb->buf_width;
  cinfo.image_height = imb->buf_height + labelHeight;

  cinfo.input_components = 3;		/* # of color components per pixel */
  cinfo.in_color_space = JCS_RGB;	/* colorspace of input image */

  jpeg_set_defaults(&cinfo);
  jpeg_set_quality( &cinfo, 100, TRUE);

  jpeg_start_compress(&cinfo, TRUE);

  //
  // TODO: Rayonix images already have the frame number in the label,
  // so don't add it for these.  How to tell?  Probably imb should
  // have a field for the detector type and/or the number of frames in
  // the file.  When the number is 1 then don't add the frame number
  // to the label.
  //

  if (labelHeight && json_string_value(json_object_get(job,"label"))) {
    if (json_integer_value(json_object_get(imb->meta, "first_frame")) == json_integer_value(json_object_get(imb->meta, "last_frame"))) {
      snprintf(label, sizeof(label)-1, "%s", json_string_value(json_object_get(job,"label")));
    } else {
      snprintf(label, sizeof(label)-1, "%s %d", json_string_value(json_object_get(job,"label")), (int)json_integer_value(json_object_get(job,"frame")));
    }

    label[sizeof(label)-1] = 0;

    isJpegLabel(label, imb->buf_width, labelHeight, &cinfo);
  }

  wval = json_integer_value(json_object_get(job,"wval"));
  bval = json_integer_value(json_object_get(job, "contrast"));
  
  //
  // Perhaps autoscale black values
  //
  if (bval <= 0) {
    bval = json_number_value(json_object_get(imb->meta, "mean")) + json_number_value(json_object_get(imb->meta, "stddev"));
  }
  
  //
  // Perhaps autoscale white values
  //
  if (wval < 0) {
    wval = json_number_value(json_object_get(imb->meta, "mean")) - json_number_value(json_object_get(imb->meta, "stddev"));
  }

  wval = wval < 0 ? 0 : wval;
  bval = bval <= wval ? wval+1 : bval;  

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

  isJpegSend(job, imb->meta, out_buffer, (int)(cinfo.dest->next_output_byte - out_buffer));

  free(row_buffer);
  //
  // out_buffer is owned by zmq and will get freed whenever it is good and ready to do that.
  //
  //free(out_buffer);

  pthread_rwlock_unlock(&imb->buflock);

  pthread_mutex_lock(&ibctx->ctxMutex);
  imb->in_use--;

  assert(imb->in_use >= 0);

  pthread_mutex_unlock(&ibctx->ctxMutex);
  return;
}
