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

json_t *decryptIsAuth(gpgme_ctx_t gpg_ctx, const char *isAuth) {
  json_t *rtn;
  json_error_t jerr;
  gpgme_error_t gpg_err;
  gpgme_data_t cipher;
  gpgme_data_t plaintext;
  char *fixedIsAuth;
  char *msg;
  int msg_size;

  fixedIsAuth = fixLineFeeds(isAuth);

  gpg_err = gpgme_data_new_from_mem( &cipher, fixedIsAuth, strlen(fixedIsAuth), 0);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "decryptIsAuth: Could not create gpg data object for cipher text: %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);

    return NULL;
  }

  gpg_err = gpgme_data_new(&plaintext);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "decryptIsAuth: Could not create gpg data object (plaintext): %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);
    gpgme_data_release(cipher);

    return NULL;
  }

  gpg_err = gpgme_op_decrypt(gpg_ctx, cipher, plaintext);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "decryptIsAuth: Failed to decrypt cipher: %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);
    gpgme_data_release(cipher);
    gpgme_data_release(plaintext);

    return NULL;
  }

  msg_size = gpgme_data_seek(plaintext, 0, SEEK_END);
  if (msg_size < 0) {
    fprintf(stderr, "decryptIsAuth: Could not seek to end of plaintext message\n");

    free(fixedIsAuth);
    gpgme_data_release(cipher);
    gpgme_data_release(plaintext);

    return NULL;
  }

  msg = calloc(1, msg_size+1);
  if (msg == NULL) {
    fprintf(stderr, "decryptIsAuth: Out of memory\n");
    exit (-1);
  }

  gpgme_data_seek(plaintext, 0, SEEK_SET);
  gpgme_data_read(plaintext, msg, msg_size);
  msg[msg_size] = 0;
        
  rtn = json_loads(msg, 0, &jerr);
  if (rtn == NULL) {
    fprintf(stderr, "decryptIsAuth: Failed to parse '%s': %s\n", msg, jerr.text);

    free(fixedIsAuth);
    free(msg);
    gpgme_data_release(cipher);
    gpgme_data_release(plaintext);

    return NULL;
  }

  free(fixedIsAuth);
  free(msg);
  gpgme_data_release(cipher);
  gpgme_data_release(plaintext);
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
void set_json_object_real(const char *cid, json_t *j, const char *key, double value) {
  static const char *id = "set_json_object_real";
  json_t *tmp_obj;
  int err;

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
