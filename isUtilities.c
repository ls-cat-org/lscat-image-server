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
    fprintf(stderr, "Could not create gpg data object for cipher text: %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);

    return NULL;
  }

  gpg_err = gpgme_data_new(&plaintext);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "Could not create gpg data object (plaintext): %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);
    gpgme_data_release(cipher);

    return NULL;
  }

  gpg_err = gpgme_op_decrypt(gpg_ctx, cipher, plaintext);
  if (gpg_err != GPG_ERR_NO_ERROR) {
    fprintf(stderr, "Failed to decrypt cipher: %s\n", gpgme_strerror(gpg_err));

    free(fixedIsAuth);
    gpgme_data_release(cipher);
    gpgme_data_release(plaintext);

    return NULL;
  }

  msg_size = gpgme_data_seek(plaintext, 0, SEEK_END);
  if (msg_size < 0) {
    fprintf(stderr, "Could not seek to end of plaintext message\n");

    free(fixedIsAuth);
    gpgme_data_release(cipher);
    gpgme_data_release(plaintext);

    return NULL;
  }

  msg = calloc(1, msg_size+1);
  if (msg == NULL) {
    fprintf(stderr, "Out of memory (msg calloc in decryptIsAuth)\n");
    exit (-1);
  }

  gpgme_data_seek(plaintext, 0, SEEK_SET);
  gpgme_data_read(plaintext, msg, msg_size);
  msg[msg_size] = 0;
        
  rtn = json_loads(msg, 0, &jerr);
  if (rtn == NULL) {
    fprintf(stderr, "Failed to parse '%s': %s\n", msg, jerr.text);

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
