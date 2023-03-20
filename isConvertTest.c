#include <stdlib.h>
#include <stdio.h>
#include <hdf5.h>
#include <jansson.h>

#include "is.h"

void usage() {
  fprintf(stderr, "isConvertTest <h5_file>\n\n");
}

/**
 * Usage: isConvertTest <h5_file>
 */
int main(int argc, char** argv) {
  if (argc != 2) {
    usage();
    return 1;
  }

  //
  // Open up the master file
  //
  hid_t master_file = H5Fopen(argv[1], H5F_ACC_RDONLY, H5P_DEFAULT);
  if (master_file < 0) {
    fprintf(stderr, "failed to open master file\n");
    return 1;
  }

  json_t* dcu_version = get_dcu_version(master_file);
  if (dcu_version == NULL) {
    fprintf(stderr, "failed to get DCU version\n");
  }
  
  const char* dcu_version_str = json_string_value( json_object_get(dcu_version, json_convert_software_version.json_name) );
  if (strcmp("1.8.0", dcu_version_str) == 0) {
    printf("Testing 1.8 compatibility\n\n");
    for (int i=0; i < json_convert_array_1_8_size; i++) {
      json_t* tmp_obj = h5_property_to_json(master_file, &json_convert_array_1_8[i]);
      if (tmp_obj != NULL) {
	printf("\n%s\n", json_dumps(tmp_obj, JSON_INDENT(2)));
      } else {
	printf("\nFAILED: %s\n", json_convert_array_1_8[i].h5_location);
      }
      json_decref(tmp_obj);
    }
    printf("\n\n");
  } else {
    printf("Testing 1.6 compatibility\n\n");
    for (int i=0; i < json_convert_array_1_6_size; i++) {
      json_t* tmp_obj = h5_property_to_json(master_file, &json_convert_array_1_6[i]);
      if (tmp_obj != NULL) {
	printf("\n%s\n", json_dumps(tmp_obj, JSON_INDENT(2)));
      } else {
	printf("\nFAILED: %s\n", json_convert_array_1_6[i].h5_location);
      }
      json_decref(tmp_obj);
    }
    printf("\n\n");
  }

  H5Fclose(master_file);
  return 0;
}
