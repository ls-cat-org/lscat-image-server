#include "is.h"

void isRayonixJpeg(json_t *job) {

  fprintf(stderr, "isRayonixJpeg: Here I am with file named %s\n", json_string_value(json_object_get(job, "fn")));
  return;
}

