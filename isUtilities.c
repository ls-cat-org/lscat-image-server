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

image_access_type isFindFile(const char *fn) {
  struct stat buf;
  image_file_type rtn;
  int fd;
  int err;
  int stat_errno;

  errno = 0;
  fd = open(fn, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "isFindFile: Could not open file %s: %s\n", fn, strerror(errno));
    return NOACCESS;
  }

  errno = 0;
  err = fstat(fd, &buf);
  stat_errno = errno;
  close(fd);

  if (err != 0) {
    fprintf(stderr, "isFindFile: Could not find file '%s': %s\n", fn, strerror(stat_errno));
    return NOACCESS;
  }
  
  if (!S_ISREG(buf.st_mode)) {
    fprintf(stderr, "isFindFile: %s is not a regular file\n", fn);
    return NOACCESS;
  }

  //
  // Walk through the readable possibilities.  Consider that the
  // ownership and group privileges may be more restrictive than the
  // world privileges.
  //
  rtn = NOACCESS;
  if ((getuid() == buf.st_uid || geteuid() == buf.st_uid) && (buf.st_mode & S_IRUSR)) {
    rtn = READABLE;
  } else {
    if ((getgid() == buf.st_gid || getegid() == buf.st_gid) && (buf.st_mode & S_IRGRP)) {
      rtn = READABLE;
    } else {
      if (buf.st_mode & S_IROTH) {
        rtn = READABLE;
      }
    }
  }

  //
  // Test for writable priveleges: we consider the result as NOACCESS
  // if the file is writable without being readable.
  //
  if (rtn == (image_file_type)READABLE) {
    if ((getuid() == buf.st_uid || geteuid() == buf.st_uid) && (buf.st_mode & S_IWUSR)) {
      rtn = WRITABLE;
    } else {
      if ((getgid() == buf.st_gid || getegid() == buf.st_gid) && (buf.st_mode & S_IWGRP)) {
        rtn = WRITABLE;
      } else {
        if (buf.st_mode & S_IWOTH) {
          rtn = WRITABLE;
        }
      }
    }
  }
  return rtn;
}

image_file_type isFileType(const char *fn) {
  htri_t ish5;
  int fd;
  int nbytes;
  unsigned int buf4;

  fd = open(fn, O_RDONLY);
  if (fd < 0) {
    fprintf(stderr, "Could not open file '%s'\n", fn);
    return UNKNOWN;
  }

  nbytes = read(fd, (char *)&buf4, 4);
  close(fd);
  if (nbytes != 4) {
    fprintf(stderr, "Could not read 4 bytes from file '%s'\n", fn);
    return UNKNOWN;
  }

  if (buf4 == 0x002a4949) {
    return RAYONIX;
  }

  if (buf4 == 0x49492a00) {
    return RAYONIX_BS;
  }

  //
  // H5 is easy
  //
  ish5 = H5Fis_hdf5(fn);
  if (ish5 > 0) {
    return HDF5;
  }

  fprintf(stderr, "isFileType: Unknown file type '%s'\n", fn);
  return UNKNOWN;
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
