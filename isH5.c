#include "is.h"

void isH5Jpeg(json_t *job) {
  fprintf(stderr, "isH5Jpeg: Here I am with file %s\n", json_string_value(json_object_get(job, "fn")));
}
