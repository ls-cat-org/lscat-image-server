/*! @file isUtilities.c
 *  @copyright 2017 by Northwestern University All Rights Reserved
 *  @author Keith Brister
 *  @brief Various and sundry utilities to support the LS-CAT Image Server Version 2
 */
#include "is.h"
//
// For some reason our isAuth strings come to us with escaped \n line
// endings instead of the unescaped \r\n line endings that gpgme
// requires;
//
char *fixLineFeeds( const char *s) {
  int i;
  const char *in;
  char *out;
  char *rtn;
  int in_newline;

  // we are replacing the two character combination '\\' 'n' with the
  // two character combination \r\n so the string sizes will be the
  // same.
  rtn = calloc(1, strlen(s) + 1);
  if (rtn == NULL) {
    fprintf(stderr, "Out of memory (fixLineFeeds)\n");
    exit (-1);
  }

  in = s;
  out = rtn;
  in_newline = 0;
  for (i=0; i<strlen(s); i++) {
    if (*in == '"') {
      in++;
      continue;
    }
    if (in_newline) {
      if (*in == 'n') {
        *(out++) = '\r';
        *(out++) = '\n';
        in++;
        in_newline = 0;
      } else {
        *(out++) = '\\';
        *(out++) = *(in++);
      }
    } else {
      if (*(in) == '\\') {
        in++;
        in_newline = 1;
      } else {
        *(out++) = *(in++);
      }
    }
  }
  return(rtn);
}


/** openssl_vase64_decode
 *  From https://github.com/exabytes18/OpenSSL-Base64/blob/master/base64.c
 *
 *  Shouldn't this be in some nice library I could include?
 */

void openssl_base64_decode(char *encoded_bytes, char **decoded_bytes, ssize_t *decoded_length) {
  BIO *bioMem, *b64;
  ssize_t buffer_length;

  bioMem = BIO_new_mem_buf((void *)encoded_bytes, -1);
  b64 = BIO_new(BIO_f_base64());
  BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
  bioMem = BIO_push(b64, bioMem);

  buffer_length = BIO_get_mem_data(bioMem, NULL);
  *decoded_bytes = malloc(buffer_length);
  *decoded_length = BIO_read(bioMem, *decoded_bytes, buffer_length);
  BIO_free_all(bioMem);
}

int verify_it(const unsigned char* msg, size_t mlen, char* sig, size_t slen, EVP_PKEY* pkey) {
  static const char *id = FILEID "verify_it";
  /* Returned to caller */
  int result = -1;
  
  if(!msg || !mlen || !sig || !slen || !pkey) {
    return -1;
  }
  
  EVP_MD_CTX* ctx = NULL;
  
  do
    {
      ctx = EVP_MD_CTX_create();
      if(ctx == NULL) {
        fprintf(stderr, "%s: EVP_MD_CTX_create failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      const EVP_MD* md = EVP_get_digestbyname("SHA256");
      if(md == NULL) {
        fprintf(stderr, "%s: EVP_get_digestbyname failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      int rc = EVP_DigestInit_ex(ctx, md, NULL);
      if(rc != 1) {
        fprintf(stderr, "%s: EVP_DigestInit_ex failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      rc = EVP_DigestVerifyInit(ctx, NULL, md, NULL, pkey);
      if(rc != 1) {
        fprintf(stderr, "%s: EVP_DigestVerifyInit failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      rc = EVP_DigestVerifyUpdate(ctx, msg, mlen);
      if(rc != 1) {
        fprintf(stderr, "%s: EVP_DigestVerifyUpdate failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      /* Clear any errors for the call below */
      ERR_clear_error();
      
      rc = EVP_DigestVerifyFinal(ctx, (unsigned char *)sig, slen);
      if(rc != 1) {
        fprintf(stderr, "%s: EVP_DigestVerifyFinal failed: %s\n", id, ERR_error_string(ERR_get_error(), NULL));
        break; /* failed */
      }
      result = 0;
    } while(0);
  
  if(ctx) {
    EVP_MD_CTX_destroy(ctx);
    ctx = NULL;
  }
  return !!result;
}

int verifyIsAuth( char *isAuth, char *isAuthSig_str) {
  static const char *id = FILEID "verifyIsAuth";
  FILE *fp;
  char *isAuthSig;
  ssize_t isAuthSig_len;
  int rtn;
  EVP_PKEY *pkey;

  openssl_base64_decode(isAuthSig_str, &isAuthSig, &isAuthSig_len);
  
  if (isAuthSig_len == 0 || isAuthSig == NULL) {
    fprintf(stderr, "%s: Could not decode isAuthSig\n", id);
    return 0;
  }

  fp = fopen("ls-ee-contrabass-pubkey.pem", "r");
  if (fp == NULL) {
    fprintf(stderr, "%s: Could not open public key\n", id);
    exit (-1);
  }
  pkey = PEM_read_PUBKEY(fp, NULL, NULL, NULL);
  fclose(fp);

  rtn = !verify_it((unsigned char *)isAuth, strlen(isAuth), isAuthSig, isAuthSig_len, pkey);
  free(isAuthSig);
  return rtn;
}


int isEsafAllowed(json_t *isAuth, int esaf) {
  json_t *allowedESAFs;
  size_t i;
  json_t *v;

  // esaf 0 is a placeholder for actions that do not require an esaf.
  // This is always allowed;

  if (esaf == 0) {
    return 1;
  }

  allowedESAFs = json_object_get(isAuth, "allowedESAFs");
  if (allowedESAFs == NULL) {
    fprintf(stderr, "isEsafAllowed: No allowedESAFs array not found\n");
    return 0;
  }

  json_array_foreach(allowedESAFs, i, v) {
    if (json_integer_value(v) == esaf) {
      return 1;
    }
  }
  return 0;
}

//
// Convenence routine for setting a string value in a json object
//
void set_json_object_string(const char *cid, json_t *j, const char *key, const char *fmt, ...) {
  static const char *id = "set_json_object_string";
  va_list arg_ptr;
  char v[PATH_MAX];
  json_t *tmp_obj;
  int err;

  va_start( arg_ptr, fmt);
  err = vsnprintf( v, sizeof(v), fmt, arg_ptr);
  v[sizeof(v)-1] = 0;
  if (err < 0 || err >= sizeof(v)) {
    fprintf(stderr, "%s->%s: Could not create temporary string for key '%s'\n", cid, id, key);
    exit (-1);
  }
  va_end( arg_ptr);

  tmp_obj = json_string(v);
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not create json object for key '%s'\n", cid, id, key);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}


//
// Convenence routine for setting an integer value in a json object
//
void set_json_object_integer(const char *cid, json_t *j, const char *key, int value) {
  static const char *id = "set_json_object_integer";
  json_t *tmp_obj;
  int err;

  tmp_obj = json_integer(value);
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not create json object for key '%s'\n", id, cid, key);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}


//
// Convenence routine for setting an integer value in a json object
//
void set_json_object_integer_array( const char *cid, json_t *j, const char *key, int values[], int n) {
  static const char *id = "set_json_object_integer_array";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  int err;
  int i;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not create json array object for key '%s'\n", cid, id, key);
    exit (-1);
  }

  for (i=0; i<n; i++) {
    tmp2_obj = json_integer(values[i]);
    if (tmp2_obj == NULL) {
      fprintf(stderr, "%s->%s: Could not create integer object for index %d\n", cid, id, i);
      exit (-1);
    }
    
    err = json_array_append_new(tmp_obj,tmp2_obj);
    if (err == -1) {
      fprintf(stderr, "%s->%s: Could not append array index %d to json_array\n", cid, id, i);
      exit (-1);
    }

  }

  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

//
// Convenence routine for setting a real value in a json object
//
void set_json_object_real(const char *cid, json_t *j, const char *key, double value) {
  static const char *id = "set_json_object_real";
  json_t *tmp_obj;
  int err;

  if (isnan(value) || isinf(value)) {
    fprintf(stderr, "%s->%s: Ignoring request for key %s with value %f\n", cid, id, key, value);
    return;
  }

  tmp_obj = json_real(value);
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not create json object for key '%s' with value %f\n", cid, id, key, value);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

/** Support for a one dimensional array of doubles
 */
void set_json_object_float_array( const char *cid, json_t *j, const char *key, float *values, int n) {
  static const char *id = "set_json_object_float_array";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  int err;
  int i;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not create json array object for key '%s'\n", cid, id, key);
    exit (-1);
  }

  for (i=0; i < n; i++) {
    tmp2_obj = json_real(values[i]);
    if (tmp2_obj == NULL) {
      fprintf(stderr, "%s->%s: Could not create integer object for index %d\n", cid, id, i);
      exit (-1);
    }
    
    err = json_array_append_new(tmp_obj,tmp2_obj);
    if (err == -1) {
      fprintf(stderr, "%s->%s: Could not append array index %d to json_array\n", cid, id, i);
      exit (-1);
    }

  }

  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

void set_json_object_float_array_2d(const char *cid, json_t *j, const char *k, float *v, int rows, int cols) {
  const static char *id = "set_json_object_float_array_2d";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  json_t *tmp3_obj;
  int row, col;
  int err;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Could not json 2d array object for key %s\n", cid, id, k);
    exit (-1);
  }

  for (col=0; col<cols; col++) {
    tmp2_obj = json_array();
    if (tmp2_obj == NULL) {
      fprintf(stderr, "%s->%s: Could not create tmp2_obj for key %s column %d\n", cid, id, k, col);
      exit (-1);
    }
    
    for (row=0; row<rows; row++) {
      tmp3_obj = json_real(*(v + rows * col + row));
      if (tmp3_obj == NULL) {
        fprintf(stderr, "%s->%s: Could not create tmp3_obj for key %s column %d row %d\n", cid, id, k, col, row);
        exit (-1);
      }

      err = json_array_append_new(tmp2_obj, tmp3_obj);
      if (err != 0) {
        fprintf(stderr, "%s->%s: Could not append value to array key %s  col %d row %d\n", cid, id, k, col, row);
        exit (-1);
      }
    }

    err = json_array_append_new(tmp_obj, tmp2_obj);
    if (err != 0) {
      fprintf(stderr, "%s->%s: Could not append column to result array key %s col %d\n", cid, id, k, col);
      exit (-1);
    }
  }

  err = json_object_set_new(j, k, tmp_obj);
  if (err != 0) {
    fprintf(stderr, "%s->%s: Could not add key %s to json object\n", cid, id, k);
    exit (-1);
  }
}


int get_integer_from_json_object(const char *cid, json_t *j, char *key) {
  static const char *id = "get_integer_from_json_object";
  json_t *tmp_obj;
  
  tmp_obj = json_object_get(j, key);
  if (tmp_obj == NULL) {
    fprintf(stderr, "%s->%s: Failed to get integer '%s' from json object\n", cid, id, key);
    exit (-1);
  }
  if (json_typeof(tmp_obj) != JSON_INTEGER) {
    fprintf(stderr, "%s->%s: json key '%s' did not hold an integer.  Got type %d\n", cid, id, key, json_typeof(tmp_obj));
    exit (-1);
  }

  return (int)json_integer_value(tmp_obj);
}

void is_zmq_free_fn(void *data, void *hint) {
  static const char *id = FILEID "is_zmq_free_fn";
  (void)id;

  if (data != NULL) {
    free(data);
  }
}

void is_zmq_error_reply(zmq_msg_t *msgs, int n_msgs, void *err_dealer, char *fmt, ...) {
  static const char *id = FILEID "is_zmq_error_reply";
  char *msg;
  int msg_len = 4096;
  va_list arg_ptr;
  zmq_msg_t zrply;
  int err;
  int i;

  // send the envelope message parts to our socket so the reply goes somewhere
  for (i=0; i<n_msgs; i++) {
    zmq_msg_send(&msgs[i], err_dealer, ZMQ_SNDMORE);
    zmq_msg_close(&msgs[i]);
  }

  msg = calloc(1, msg_len);
  if (msg == NULL) {
    fprintf(stderr, "%s: Out of memory\n", id);
    exit (-1);
  }

  va_start( arg_ptr, fmt);
  vsnprintf( msg, msg_len-1, fmt, arg_ptr);
  va_end( arg_ptr);
  msg[msg_len-1]=0;
      
  zmq_msg_init(&zrply);
  err = zmq_msg_init_data(&zrply, msg, strlen(msg), is_zmq_free_fn, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Could not create reply message: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_msg_send(&zrply, err_dealer, 0);
  if (err == -1) {
    fprintf(stderr, "%s: could not send reply (zrply 1): %s\n", id, zmq_strerror(errno));
  }
}
