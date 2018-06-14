/*! @file isUtilities.c
 *  @copyright 2017 by Northwestern University All Rights Reserved
 *  @author Keith Brister
 *  @brief Various and sundry utilities to support the LS-CAT Image Server Version 2
 */
#include "is.h"

/** For some reason our isAuth strings come to us with escaped \\n line
 ** endings instead of the unescaped \\r\\n line endings that gpgme
 ** requires.
 ** 
 ** @param[in] s String to fix
 **
 ** @returns fixed string.  Call "free" when done with it.
*/
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
    isLogging_crit("Out of memory (fixLineFeeds)\n");
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
 ** From https://github.com/exabytes18/OpenSSL-Base64/blob/master/base64.c
 **
 ** Shouldn't this be in some nice library I could include?
 **
 ** @param[in] encoded_bytes   String that we'd like to decode
 **
 ** @param[out] decoded_bytes The happy decoded string
 **
 ** @param[out] decoded_length As the name suggests, this is the length of our decoded string.
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

/** Verify a signature
 **
 ** @param msg    Message to verify
 **
 ** @param mlen   Length of our message msg
 **
 ** @param sig    Signature to verify against
 **
 ** @param slen   Length of our signature sig
 **
 ** @param pkey   Public key of the signer
 **
 ** @returns -1 on failed verification, 0 on success
 */
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
        isLogging_err("%s: EVP_MD_CTX_create failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      const EVP_MD* md = EVP_get_digestbyname("SHA256");
      if(md == NULL) {
        isLogging_err("%s: EVP_get_digestbyname failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      int rc = EVP_DigestInit_ex(ctx, md, NULL);
      if(rc != 1) {
        isLogging_err("%s: EVP_DigestInit_ex failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      rc = EVP_DigestVerifyInit(ctx, NULL, md, NULL, pkey);
      if(rc != 1) {
        isLogging_err("%s: EVP_DigestVerifyInit failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      rc = EVP_DigestVerifyUpdate(ctx, msg, mlen);
      if(rc != 1) {
        isLogging_err("%s: EVP_DigestVerifyUpdate failed, error 0x%lx\n", id, ERR_get_error());
        break; /* failed */
      }
      
      /* Clear any errors for the call below */
      ERR_clear_error();
      
      rc = EVP_DigestVerifyFinal(ctx, (unsigned char *)sig, slen);
      if(rc != 1) {
        isLogging_err("%s: EVP_DigestVerifyFinal failed: %s\n", id, ERR_error_string(ERR_get_error(), NULL));
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

/** Check that the isAuth message is valid
 **
 ** @param[in] isAuth        Plain text string we'd like to verify
 **
 ** @param[in] isAuthSig_str Encrypted signature of the message
 **
 ** @returns 0 on failure, 1 on success
 **
 */
int verifyIsAuth( char *isAuth, char *isAuthSig_str) {
  static const char *id = FILEID "verifyIsAuth";
  FILE *fp;
  char *isAuthSig;
  ssize_t isAuthSig_len;
  int rtn;
  EVP_PKEY *pkey;

  openssl_base64_decode(isAuthSig_str, &isAuthSig, &isAuthSig_len);
  
  if (isAuthSig_len == 0 || isAuthSig == NULL) {
    isLogging_err("%s: Could not decode isAuthSig\n", id);
    return 0;
  }

  fp = fopen("ls-ee-contrabass-pubkey.pem", "r");
  if (fp == NULL) {
    isLogging_err("%s: Could not open public key\n", id);
    exit (-1);
  }
  pkey = PEM_read_PUBKEY(fp, NULL, NULL, NULL);
  fclose(fp);

  rtn = !verify_it((unsigned char *)isAuth, strlen(isAuth), isAuthSig, isAuthSig_len, pkey);
  free(isAuthSig);
  return rtn;
}


/** Check that an ESAF is allowed by isAuth.
 ** Here we assume that isAuth's signature has been verified.
 ** 
 ** @param isAuth   JSON object containing ESAFS we are allowed to use, among other things.
 **
 ** @param esaf     ESAF number we wish to check
 **
 ** @returns 1 on esaf allowed, 0 on esaf not allowed
 */
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
    isLogging_err("isEsafAllowed: No allowedESAFs array not found\n");
    return 0;
  }

  json_array_foreach(allowedESAFs, i, v) {
    if (json_integer_value(v) == esaf) {
      return 1;
    }
  }
  return 0;
}

/** Convenence routine for setting a string value in a json object.
 **
 ** @param[in]     cid String identifing the calling routine for use in error messages
 **
 ** @param[in,out] j   JSON object in which we are setting the key
 **
 ** @param[in]     key Sure enough, this is the name of the key
 **
 ** @param[in]     fmt printf style format string
 **
 ** @param[in]     ... Arguments as specified by fmt
 **
 */
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
    isLogging_err("%s->%s: Could not create temporary string for key '%s'\n", cid, id, key);
    exit (-1);
  }
  va_end( arg_ptr);

  tmp_obj = json_string(v);
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not create json object for key '%s'\n", cid, id, key);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}



/** Convenence routine for setting an integer value in a json object
 **
 ** @param[in] cid    String identifying the calling routine.  Used for error messages.
 **
 ** @param[in,out] j  JSON object to modify
 **
 ** @param[in] key    Name of the key to add to j
 **
 ** @param[in] value  Integer value to add
 */
void set_json_object_integer(const char *cid, json_t *j, const char *key, int value) {
  static const char *id = "set_json_object_integer";
  json_t *tmp_obj;
  int err;

  tmp_obj = json_integer(value);
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not create json object for key '%s'\n", id, cid, key);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}


/** Convenence routine for setting an integer value in a json object
 **
 ** @param cid    Name of calling routine for error messages
 **
 ** @param j      JSON object to modify
 **
 ** @param key    Key to add to j
 **
 ** @param values Array of integers to add under key
 **
 ** @param n      Number of elements in the array "values"
 */
void set_json_object_integer_array( const char *cid, json_t *j, const char *key, int values[], int n) {
  static const char *id = "set_json_object_integer_array";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  int err;
  int i;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not create json array object for key '%s'\n", cid, id, key);
    exit (-1);
  }

  for (i=0; i<n; i++) {
    tmp2_obj = json_integer(values[i]);
    if (tmp2_obj == NULL) {
      isLogging_err("%s->%s: Could not create integer object for index %d\n", cid, id, i);
      exit (-1);
    }
    
    err = json_array_append_new(tmp_obj,tmp2_obj);
    if (err == -1) {
      isLogging_err("%s->%s: Could not append array index %d to json_array\n", cid, id, i);
      exit (-1);
    }

  }

  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

/** Convenence routine for setting a real value in a json object
 **
 ** @param cid    Name of calling routine for constructing error messages
 **
 ** @param j      JSON object to modify
 ** 
 ** @param key    Key to add to j
 **
 ** @param value  Value to assign to the key
 **
 */
void set_json_object_real(const char *cid, json_t *j, const char *key, double value) {
  static const char *id = "set_json_object_real";
  json_t *tmp_obj;
  int err;

  if (isnan(value) || isinf(value)) {
    isLogging_err("%s->%s: Ignoring request for key %s with value %f\n", cid, id, key, value);
    return;
  }

  tmp_obj = json_real(value);
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not create json object for key '%s' with value %f\n", cid, id, key, value);
    exit (-1);
  }
  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

/** Support for a one dimensional array of doubles
 **
 ** @param cid     Name of calling routine
 **
 ** @param j       JSON object to modify
 **
 ** @param key     Key to modify or add to j
 **
 ** @param values  One dimensional array of floats
 **
 ** @param n       Number of elements in values
 */
void set_json_object_float_array( const char *cid, json_t *j, const char *key, float *values, int n) {
  static const char *id = "set_json_object_float_array";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  int err;
  int i;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not create json array object for key '%s'\n", cid, id, key);
    exit (-1);
  }

  for (i=0; i < n; i++) {
    tmp2_obj = json_real(values[i]);
    if (tmp2_obj == NULL) {
      isLogging_err("%s->%s: Could not create integer object for index %d\n", cid, id, i);
      exit (-1);
    }
    
    err = json_array_append_new(tmp_obj,tmp2_obj);
    if (err == -1) {
      isLogging_err("%s->%s: Could not append array index %d to json_array\n", cid, id, i);
      exit (-1);
    }

  }

  err = json_object_set_new(j, key, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key '%s' to json object\n", cid, id, key);
    exit (-1);
  }
}

/** Convenience routine to add a 2d array of floats to a json object.
 **
 ** @param cid   Name of calling routine
 **
 ** @param j     JSON object to modify
 **
 ** @param k     Key to place the array in
 **
 ** @param v     Our array
 **
 ** @param rows  Number of rows
 ** 
 ** @param cols  Number of columns
 */
void set_json_object_float_array_2d(const char *cid, json_t *j, const char *k, float *v, int rows, int cols) {
  const static char *id = "set_json_object_float_array_2d";
  json_t *tmp_obj;
  json_t *tmp2_obj;
  json_t *tmp3_obj;
  int row, col;
  int err;

  tmp_obj = json_array();
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Could not json 2d array object for key %s\n", cid, id, k);
    exit (-1);
  }

  for (col=0; col<cols; col++) {
    tmp2_obj = json_array();
    if (tmp2_obj == NULL) {
      isLogging_err("%s->%s: Could not create tmp2_obj for key %s column %d\n", cid, id, k, col);
      exit (-1);
    }
    
    for (row=0; row<rows; row++) {
      tmp3_obj = json_real(*(v + rows * col + row));
      if (tmp3_obj == NULL) {
        isLogging_err("%s->%s: Could not create tmp3_obj for key %s column %d row %d\n", cid, id, k, col, row);
        exit (-1);
      }

      err = json_array_append_new(tmp2_obj, tmp3_obj);
      if (err != 0) {
        isLogging_err("%s->%s: Could not append value to array key %s  col %d row %d\n", cid, id, k, col, row);
        exit (-1);
      }
    }

    err = json_array_append_new(tmp_obj, tmp2_obj);
    if (err != 0) {
      isLogging_err("%s->%s: Could not append column to result array key %s col %d\n", cid, id, k, col);
      exit (-1);
    }
  }

  err = json_object_set_new(j, k, tmp_obj);
  if (err != 0) {
    isLogging_err("%s->%s: Could not add key %s to json object\n", cid, id, k);
    exit (-1);
  }
}

/** convenence routine to extract an integer from an object
 **
 ** Without this the caller would have to generate a temporary json
 ** object to hold the value.
 **
 ** @param cid   Name of calling routine
 **
 ** @param j     JSON object hiding our value
 **
 ** @param key   Key corresponding to our integer
 **
 ** @return Value of said integer.
 */
int get_integer_from_json_object(const char *cid, json_t *j, char *key) {
  static const char *id = "get_integer_from_json_object";
  json_t *tmp_obj;
  
  tmp_obj = json_object_get(j, key);
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Failed to get integer '%s' from json object\n", cid, id, key);
    exit (-1);
  }
  if (json_typeof(tmp_obj) != JSON_INTEGER) {
    isLogging_err("%s->%s: json key '%s' did not hold an integer.  Got type %d\n", cid, id, key, json_typeof(tmp_obj));
    exit (-1);
  }

  return (int)json_integer_value(tmp_obj);
}

/** Extract a double value from a json object
 *
 *  @param[in]  j    Json object from which the double value should be extracted
 *
 *  @param[in]  key  Name of the attribute to extract
 *
 *  @returns Value of the named attribute.
 *
 *  @remark  All errors fatal.
 */
double get_double_from_json_object(const char *cid,  const json_t *j, const char *key) {
  static const char *id = "get_double_from_json_object";
  json_t *tmp_obj;
  
  tmp_obj = json_object_get(j, key);
  if (tmp_obj == NULL) {
    isLogging_err("%s->%s: Failed to get number '%s' from json object\n", cid, id, key);
    exit (-1);
  }

  if (json_typeof(tmp_obj) != JSON_REAL && json_typeof(tmp_obj) != JSON_INTEGER) {
    isLogging_err("%s->%s: json key '%s' did not hold an integer or a real.  Got type %d\n", cid, id, key, json_typeof(tmp_obj));
    exit (-1);
  }

  return json_number_value(tmp_obj);
}



/** ZMQ needs us to pass a free routine to free data whenever it is done with it.
 **
 ** @param data   data to free
 **
 ** @param hint   opaque pointer to use at our whim.  It's unused
 */
void is_zmq_free_fn(void *data, void *hint) {
  static const char *id = FILEID "is_zmq_free_fn";
  (void)id;

  if (data != NULL) {
    free(data);
  }
}

/** Routine to send error messages back through the ZMQ maze in case we have nothing real to send
 **
 ** @param msgs         List of messages to send
 **
 ** @param n_msgs        Number of messsages in our list
 **
 ** @param err_dealer   ZMQ dealer that handles messages
 **
 ** @param fmt          printf style format string
 **
 ** @param ...          parameters as specified by fmt
 **
 */
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
    isLogging_crit("%s: Out of memory\n", id);
    exit (-1);
  }

  va_start( arg_ptr, fmt);
  vsnprintf( msg, msg_len-1, fmt, arg_ptr);
  va_end( arg_ptr);
  msg[msg_len-1]=0;
      
  zmq_msg_init(&zrply);
  err = zmq_msg_init_data(&zrply, msg, strlen(msg), is_zmq_free_fn, NULL);
  if (err == -1) {
    isLogging_err("%s: Could not create reply message: %s\n", id, zmq_strerror(errno));
    exit (-1);
  }

  err = zmq_msg_send(&zrply, err_dealer, 0);
  if (err == -1) {
    isLogging_err("%s: could not send reply (zrply 1): %s\n", id, zmq_strerror(errno));
  }
}


herr_t is_h5_walker(unsigned n, const H5E_error2_t *err_desc, void *dummy) {
  isLogging_err("%d file: %s function: %s line: %d  desc: %s", n, err_desc->file_name, err_desc->func_name, err_desc->line, err_desc->desc);
  return 0;
}

int is_h5_error_handler(hid_t estack_id, void *dummy) {
  static const char *id = FILEID "is_h5_error_handler";
  herr_t herr;

  herr = H5Ewalk2(estack_id, H5E_WALK_DOWNWARD, is_h5_walker, dummy);
  if (herr < 0) {
    isLogging_err("%s: Failed to walk the h5 errors\n", id);
  }
  return 0;
}


/** Return the file name (ie.\ basename) of a path.
 *
 *  Although similar to the existing basename verions this routine
 *  does not modify its argument.
 *
 *  @param parent_id Name of the calling routing (for error reporting)
 *
 *  @param path The path from which to extract the file name.
 *
 *  @returns Pointer to file name. You must free the result when you
 *  are done with it.
 *
 *  @remark There are already two incompatiable versions of basename
 * in glibc/posix.  Let's not add a third function with the same name.
 *
 * @remark If memory cannot be allocated for the return value this
 * function causes the program to exit.
 */

char *file_name_component(const char *parent_id, const char *path) {
  static const char *id = FILEID "file_name_component";
  const char *rslash;
  char *rtn;

  //
  // Here are the pathological cases
  //
  if (path == NULL || strcmp(path,".")==0 || strcmp(path,"..")==0) {
    errno = 0;
    rtn = strdup("");
    if (rtn == NULL) {
      isLogging_err("%s->%s: Could not return pathological file component for path '%s': %s", parent_id, id, path, strerror(errno));
      exit (-1);
    }
    return rtn;
  }

  //
  // Find the right most / char
  //
  rslash = strrchr(path, '/');
  if (rslash == NULL) {
    rslash = path-1;
  }
  errno = 0;
  rtn = strdup(rslash+1);
  if (rtn == NULL) {
    isLogging_err("%s->%s: strdup failed finding file name component for path '%s': %s", parent_id, id, path, strerror(errno));
    exit (-1);
  }
  return rtn;
}

