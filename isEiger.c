// Copyright 2017 by Northwestern University
// Author: Keith Brister
//
// Services image server requests for eiger images
//
//	gcc -Wall isEiger.c -o isEiger -lhdf5 -lhiredis -llz4 -ljansson -lpthread
//
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <lz4.h>
#include <hdf5.h>
#include <hiredis/hiredis.h>
#include <jansson.h>

#define EIGER_9M_IMAGE_WIDTH  3110
#define EIGER_9M_IMAGE_HEIGHT 3269
#define EIGER_9M_IMAGE_DEPTH  4
#define IMAGE_WIDTH EIGER_9M_IMAGE_WIDTH
#define IMAGE_HEIGHT EIGER_9M_IMAGE_HEIGHT
#define IMAGE_DEPTH EIGER_9M_IMAGE_DEPTH
#define IMAGE_SIZE   (IMAGE_HEIGHT * IMAGE_WIDTH * IMAGE_DEPTH)
#define COMPRESSED_BUFFER_SIZE LZ4_COMPRESSBOUND(IMAGE_SIZE)

typedef struct frame_discovery_struct {
  struct frame_discovery_struct *next;
  hid_t data_set;
  hid_t file_space;
  hid_t file_type;
  int32_t first_frame;
  int32_t last_frame;
  char *done_list;
} frame_discovery_t;
frame_discovery_t *frame_discovery_base = NULL;

typedef struct h5_to_json_struct {
  char *h5_location;
  char *json_property_name;
  char type;
} h5_to_json_t;

h5_to_json_t json_convert_array[] = {
  { "/entry/instrument/detector/detectorSpecific/auto_summation",                  "auto_summation",                                  'i'},
  { "/entry/instrument/detector/beam_center_x",                                    "beam_center_x",                                   'f'},
  { "/entry/instrument/detector/beam_center_y",                                    "beam_center_y",                                   'f'},
  { "/entry/instrument/detector/bit_depth_readout",                                "bit_depth_readout",                               'i'},
  { "/entry/instrument/detector/detectorSpecific/calibration_type",                "calibration_type",                                's'},
  { "/entry/sample/goniometer/chi_increment",                                      "chi_increment",                                   'f'},
  { "/entry/sample/goniometer/chi_start",                                          "chi_start",                                       'f'},
  { "/entry/instrument/detector/count_time",                                       "count_time",                                      'f'},
  { "/entry/instrument/detector/detectorSpecific/countrate_correction_bunch_mode", "countrate_correction_bunch_mode",                 's'},
  { "/entry/instrument/detector/detectorSpecific/data_collection_date",            "data_collection_date",                            's'},
  { "/entry/instrument/detector/description",                                      "description",                                     's'},
  { "/entry/instrument/detector/detector_distance",                                "detector_distance",                               'f'},
  { "/entry/instrument/detector/detector_number",                                  "detector_number",                                 's'},
  { "/entry/instrument/detector/geometry/orientation/value",                       "detector_orientation",                            'F'},
  { "/entry/instrument/detector/detectorSpecific/detector_readout_period",         "detector_readout_period",                         'f'},
  { "/entry/instrument/detector/detector_readout_time",                            "detector_readout_time",                           'f'},
  { "/entry/instrument/detector/geometry/translation/distances",                   "detector_translation",                            'F'},
  { "/entry/instrument/detector/efficiency_correction_applied",                    "efficiency_correction_applied",                   'i'},
  { "/entry/instrument/detector/detectorSpecific/element",                         "element",                                         's'},
  { "/entry/instrument/detector/flatfield_correction_applied",                     "flatfield_correction_applied",                    'i'},
  { "/entry/instrument/detector/detectorSpecific/frame_count_time",                "frame_count_time",                                'f'},
  { "/entry/instrument/detector/detectorSpecific/frame_period",                    "frame_period",                                    'f'},
  { "/entry/instrument/detector/frame_time",                                       "frame_time",                                      'f'},
  { "/entry/sample/goniometer/kappa_increment",                                    "kappa_increment",                                 'f'},
  { "/entry/sample/goniometer/kappa_start",                                        "kappa_start",                                     'f'},
  { "/entry/instrument/detector/detectorSpecific/nframes_sum",                     "nframes_sum",                                     'i'},
  { "/entry/instrument/detector/detectorSpecific/nimages",                         "nimages",                                         'i'},
  { "/entry/instrument/detector/detectorSpecific/ntrigger",                        "ntrigger",                                        'i'},
  { "/entry/instrument/detector/detectorSpecific/number_of_excluded_pixels",       "number_of_excluded_pixels",                       'i'},
  { "/entry/sample/goniometer/omega_increment",                                    "omega_increment",                                 'i'},
  { "/entry/sample/goniometer/omega_start",                                        "omega_start",                                     'f'},
  { "/entry/sample/goniometer/phi_increment",                                      "phi_increment",                                   'f'},
  { "/entry/sample/goniometer/phi_start",                                          "phi_start",                                       'f'},
  { "/entry/instrument/detector/detectorSpecific/photon_energy",                   "photon_energy",                                   'f'},
  { "/entry/instrument/detector/pixel_mask_applied",                               "pixel_mask_applied",                              'i'},
  { "/entry/instrument/detector/sensor_material",                                  "sensor_material",                                 's'},
  { "/entry/instrument/detector/sensor_thickness",                                 "sensor_thickness",                                'f'},
  { "/entry/instrument/detector/detectorSpecific/software_version",                "software_version",                                's'},
  { "/entry/instrument/detector/detectorSpecific/summation_nimages",               "summation_nimages",                               'i'},
  { "/entry/instrument/detector/threshold_energy",                                 "threshold_energy",                                'f'},
  { "/entry/instrument/detector/detectorSpecific/trigger_mode",                    "trigger_mode",                                    's'},
  { "/entry/instrument/detector/goniometer/two_theta_increment",                   "two_theta_increment",                             'f'},
  { "/entry/instrument/detector/goniometer/two_theta_start",                       "two_theta_start",                                 'f'},
  { "/entry/instrument/detector/virtual_pixel_correction_applied",                 "virtual_pixel_correction_applied",                'i'},
  { "/entry/instrument/beam/incident_wavelength",                                  "wavelength",                                      'f'},
  { "/entry/instrument/detector/x_pixel_size",                                     "x_pixel_size",                                    'f'},
  { "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",            "x_pixels_in_detector",                            'i'},
  { "/entry/instrument/detector/y_pixel_size",                                     "y_pixel_size",                                    'f'},
  { "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",            "y_pixels_in_detector",                            'i'}
};

void our_free(void *ptr, void *hint) {
  free(ptr);
}

//
// the maximum number of compressed buffers is 64 since we use a 64
// bit mask to mark which buffers are in use
//

static char data_buffer[IMAGE_SIZE];
static unsigned long compressed_buffer_mask      = 0;   // high bits mark which buffers are available
static char *compressed_buffers[N_COMPRESSED_BUFFERS];
static pthread_mutex_t compressed_buffer_mutex;
static pthread_cond_t  compressed_buffer_cond;


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

/** output part one of the header
 */

void header_part1(void *skt, int series) {
  static const char *id = "header_part1";
  int err;
  json_t *h;
  char *s;
  zmq_msg_t msg;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "Could not create json_object (%s)\n", id);
    exit (-1);
  }
  
  set_json_object_string( id, h,  "htype", "dheader-1.0");
  set_json_object_integer(id, h, "series", series);
  set_json_object_string( id, h,  "header_detail", "all");

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "Failed to dump string (%s)\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "zmq_msg_init_data failed (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}


/** output header part 2: the metadata
 */
void header_part2(void *skt, json_t *meta) {
  static const char *id = "header_part2";
  int err;
  char *s;
  zmq_msg_t msg;

  s = json_dumps(meta,0);
  if (s == NULL) {
    fprintf(stderr, "Failed to generate string value of metadata (%s)\n", id);
    exit (-1);
  }
  
  errno = 0;
  err   = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}

/** output header part3: flatfield header
 */
void header_part3(void *skt, int width, int height) {
  static const char *id = "header_part3";
  json_t *h;
  int err;
  int array[2];
  char *s;
  zmq_msg_t msg;

  array[0] = width;
  array[1] = height;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "Could not create json_object (%s)\n", id);
    exit (-1);
  }
  
  set_json_object_string(id, h,  "htype", "dflatfield-1.0");
  set_json_object_integer_array(id, h, "shape", array, 2);
  set_json_object_string(id, h, "type", "float32");

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "%s: Could not dump json object\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Could not create zmq_msg\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to send message: %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}

/** output header part 4: flatfield
 */
void header_part4(void *skt, void *buf, int n) {
  static const char *id = "header_part4";
  int err;
  zmq_msg_t msg;

  errno = 0;
  err = zmq_msg_init_data(&msg, buf, n, our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Failed to create message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}


/** output header part 5: pixel mask header
 */
void header_part5(void *skt, int width, int height) {
  static const char *id = "header_part5";
  json_t *h;
  int err;
  int array[2];
  char *s;
  zmq_msg_t msg;

  array[0] = width;
  array[1] = height;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "%s: Could not create json_object\n", id);
    exit (-1);
  }
  
  set_json_object_string(id, h,  "htype", "dpixelmask-1.0");
  set_json_object_integer_array(id, h, "shape", array, 2);
  set_json_object_string(id, h, "type", "uint32");

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "%s: Could not dump json object\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Could not create zmq_msg\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to send message: %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}

/** output header part 6: pixel mask
 */
void header_part6(void *skt, void *buf, int n) {
  static const char *id = "header_part6";
  int err;
  zmq_msg_t msg;

  errno = 0;
  err = zmq_msg_init_data(&msg, buf, n, our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Failed to create message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}

/** output header part 7: count rate header
 */
void header_part7(void *skt, int rows) {
  static const char *id = "header_part7";
  json_t *h;
  int err;
  int array[2];
  char *s;
  zmq_msg_t msg;

  array[0] = 2;
  array[1] = rows;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "%s: Could not create json_object\n", id);
    exit (-1);
  }
  
  set_json_object_string(id, h,  "htype", "dcountrate_table-1.0");
  set_json_object_integer_array(id, h, "shape", array, 2);
  set_json_object_string(id, h, "type", "uint32");

  s = json_dumps(h, 0);
  if (s == NULL) {
    fprintf(stderr, "%s: Could not dump json object\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Could not create zmq_msg\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to send message: %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}

/** output header part 8: countrate table
 */
void header_part8(void *skt, void *buf, int n) {
  static const char *id = "header_part8";
  int err;
  zmq_msg_t msg;

  errno = 0;
  err = zmq_msg_init_data(&msg, buf, n, our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Failed to create message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}

void header_appendix(void *skt, json_t *h) {
  static const char *id = "header_appendix";
  char *s;
  int err;
  zmq_msg_t msg;

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "Could not dump appendix header (%s)\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Failed to create message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, 0);  // The last message part
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}

/** Data part1: header
 */

void data_part1(void *skt, int series, int frame, char *hash) {
  static const char *id = "data_part1";
  int err;
  json_t *h;
  char *s;
  zmq_msg_t msg;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "Could not create json_object (%s)\n", id);
    exit (-1);
  }
  
  set_json_object_string( id, h,  "htype", "dimage-1.0");
  set_json_object_integer(id, h, "series", series);
  set_json_object_integer(id, h, "frame", frame);
  set_json_object_string( id, h,  "hash",  hash);

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "Failed to dump string (%s)\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "zmq_msg_init_data failed (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}

/** Data part2: image header
 */
void data_part2(void *skt, int width, int height, char *type, char *encoding, int size) {
  static const char *id = "data_part2";
  json_t *h;
  int err;
  char *s;
  zmq_msg_t msg;
  int array[2];

  array[0] = width;
  array[1] = height;

  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "Could not create json_object (%s)\n", id);
    exit (-1);
  }

  set_json_object_string( id, h, "htype", "dimage_d-1.0");
  set_json_object_integer_array(id, h, "shape", array, 2);
  set_json_object_string( id, h, "type", type);
  set_json_object_string( id, h, "encoding", encoding);
  set_json_object_integer(id, h, "size", size);

  s = json_dumps(h,0);
  if (s == NULL) {
    fprintf(stderr, "Could not dump json object (%s)\n", id);
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Could not create zmq message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  json_decref(h);
}

/** Data part3: the image
 */
void data_part3( void *skt, char *buf, int n) {
  static const char *id = "data_part3";
  zmq_msg_t msg;
  int err;

  errno = 0;
  err = zmq_msg_init_data(&msg, buf, n, data_free, NULL);
  if (err == -1) {
    fprintf(stderr, "Could not create zmq message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "Failed to send message (%s): %s\n", id, strerror(errno));
    exit (-1);
  }
}

/** Data part 4: the timings
 */
void data_part4(void *skt, long long start_time, long long stop_time, long real_time) {
  static const char *id = "data_part4";
  zmq_msg_t msg;
  json_t *h;
  int err;
  char *s;
  (void)id;
  
  h = json_object();
  if (h == NULL) {
    fprintf(stderr, "%s: Could not create json object\n", id);
    exit (-1);
  }
  
  set_json_object_string( id, h,  "htype",     "dconfig-1.0");
  set_json_object_integer(id, h, "start_time", start_time);
  set_json_object_integer(id, h, "stop_time",  stop_time);
  set_json_object_integer(id, h, "real_time",  real_time);

  s = json_dumps(h, 0);
  if (s == NULL) {
    fprintf(stderr, "%s: Could dump json object\n", id);
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to create zmq message: %s\n", id, strerror(errno));
    exit (-1);
  }
  
  errno = 0;
  err = zmq_msg_send(&msg, skt, ZMQ_SNDMORE);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to send zmq message\n", id);
    exit (-1);
  }
  
  json_decref(h);
}

/** Data Appendix
 */
void data_appendix(void *skt, json_t *appendix) {
  static const char *id = "data_appendix";
  zmq_msg_t msg;
  char *s;
  int err;

  (void) id;

  s = json_dumps(appendix, 0);
  if (s == NULL) {
    fprintf(stderr, "%s: Failed to dump json appendix\n", id);
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_init_data(&msg, s, strlen(s), our_free, NULL);
  if (err == -1) {
    fprintf(stderr, "%s: Failed to create zmq message: %s\n", id, strerror(errno));
    exit (-1);
  }

  errno = 0;
  err = zmq_msg_send(&msg, skt, 0);     // Last message part
  if (err == -1) {
    fprintf(stderr, "%s: Failed to send zmq message\n", id);
    exit (-1);
  }
}

int discovery_cb(hid_t lid, const char *name, const H5L_info_t *info, void *op_data) {
  char s[256];
  const char *fnp;
  const char *pp;
  herr_t herr;
  hid_t image_nr_high;
  hid_t image_nr_low;
  frame_discovery_t *these_frames, *fp, *fpp;

  these_frames = calloc(sizeof(frame_discovery_t), 1);
  if (these_frames == NULL) {
    fprintf(stderr, "Out of memory (these_frames)\n");
    exit (-1);
  }

  //
  // setting next is more trouble than normal since we want to keep
  // the data in the same order that we were called.
  //

  fpp = NULL;
  for (fp = frame_discovery_base; fp != NULL; fp = fp->next) {
    fpp = fp;
  }

  these_frames->next = NULL;
  if (fpp == NULL) {
    frame_discovery_base = these_frames;
  } else {
    fpp->next = these_frames;
  }

  if (info->type == H5L_TYPE_EXTERNAL) {
    herr = H5Lget_val(lid, name, s, sizeof(s), H5P_DEFAULT);
    if (herr < 0) {
      fprintf(stderr, "Could not get link value %s\n", name);
      exit (-1);
    }

    herr = H5Lunpack_elink_val(s, sizeof(s), 0, &fnp, &pp);
    if (herr < 0) {
      fprintf(stderr, "Could not unpack link value for %s\n", name);
      exit (-1);
    }    
  }
  
  these_frames->data_set = H5Dopen2(lid, name, H5P_DEFAULT);
  if (these_frames->data_set < 0) {
    fprintf(stderr, "Failed to open dataset %s\n", name);
    exit (-1);
  }

  these_frames->file_space = H5Dget_space(these_frames->data_set);
  if (these_frames->file_space < 0) {
    fprintf(stderr, "Could not get data_set space for %s\n", name);
    exit (-1);
  }

  these_frames->file_type = H5Dget_type(these_frames->data_set);
  if (these_frames->file_type < 0) {
    fprintf(stderr, "Could not get data_set type for %s\n", name);
    exit (-1);
  }

  image_nr_high = H5Aopen_by_name( lid, name, "image_nr_high", H5P_DEFAULT, H5P_DEFAULT);
  if (image_nr_high < 0) {
    fprintf(stderr, "Could not open attribute 'image_nr_high' in linked file %s\n", name);
    exit (-1);
  }
  
  herr = H5Aread(image_nr_high, H5T_NATIVE_INT, &(these_frames->last_frame));
  if (herr < 0) {
    fprintf(stderr, "Could not read value 'image_nr_high' in linked file %s\n", name);
    exit (-1);
  }

  herr = H5Aclose(image_nr_high);
  if (herr < 0) {
    fprintf(stderr, "Failed to close attribute image_nr_high\n");
    exit (-1);
  }

  image_nr_low = H5Aopen_by_name( lid, name, "image_nr_low", H5P_DEFAULT, H5P_DEFAULT);
  if (image_nr_low < 0) {
    fprintf(stderr, "Could not open attribute 'image_nr_low' in linked file %s\n", name);
    exit (-1);
  }
  
  herr = H5Aread(image_nr_low, H5T_NATIVE_INT, &(these_frames->first_frame));
  if (herr < 0) {
    fprintf(stderr, "Could not read value 'image_nr_low' in linked file %s\n", name);
    exit (-1);
  }

  herr = H5Aclose(image_nr_low);
  if (herr < 0) {
    fprintf(stderr, "Failed to close attribute image_nr_low\n");
    exit (-1);
  }

  these_frames->done_list = calloc( these_frames->last_frame - these_frames->first_frame + 1, 1);
  if (these_frames->done_list == NULL) {
    fprintf(stderr, "Out of memory (done_list)\n");
    exit (-1);
  }

  return 0;
}


json_t *get_json( hid_t master_file, h5_to_json_t *htj) {
  static char *id = "get_json";
  json_t *rtn;
  herr_t herr;
  hid_t data_set;
  hid_t data_type;
  hid_t data_space;
  hid_t mem_type;
  int32_t i_value;
  float   f_value;
  float  *fa_value;
  char   *s_value;
  hsize_t value_length;
  hsize_t *dims;
  hsize_t npoints;
  int rank;

  rtn = json_object();
  if (rtn == NULL) {
    fprintf(stderr, "%s: Failed to create return object\n", id);
    exit (-1);
  }

  data_set = H5Dopen2( master_file, htj->h5_location, H5P_DEFAULT);
  if (data_set < 0) {
    fprintf(stderr, "%s: Could not open data_set %s\n", id, htj->h5_location);
    exit (-1);
  }

  switch(htj->type) {
  case 'i':
    herr = H5Dread(data_set, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &i_value);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not read %s\n", id, htj->h5_location);
      exit (-1);
    }
    set_json_object_integer(id, rtn, htj->json_property_name, i_value);
    break;

  case 'f':
    herr = H5Dread(data_set, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &f_value);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not read %s\n", id, htj->h5_location);
      exit (-1);
    }
    set_json_object_real(id, rtn, htj->json_property_name, f_value);
    break;

  case 's':
    data_type = H5Dget_type(data_set);
    if (data_type < 0) {
      fprintf(stderr, "%s: Could not get data_type (%s)\n", id, htj->h5_location);
      exit (-1);
    }
    value_length = H5Tget_size(data_type);

    if (value_length < 0) {
      fprintf(stderr, "%s: Could not determine length of string for dataset %s\n", id, htj->h5_location);
      exit (-1);
    }
    s_value = calloc(value_length+1, 1);
    if (s_value == NULL) {
      fprintf(stderr, "%s: Out of memory (s_value)\n", id);
      exit (-1);
    }
    
    mem_type = H5Tcopy(H5T_C_S1);
    if (mem_type < 0) {
      fprintf(stderr, "%s: Could not copy type for %s\n", id, htj->h5_location);
      exit (-1);
    }

    herr = H5Tset_size(mem_type, value_length);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not set memory type size (%s)\n", id, htj->h5_location);
      exit (-1);
    }

    herr = H5Dread(data_set, mem_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, s_value);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not read %s\n", id, htj->h5_location);
      exit (-1);
    }

    set_json_object_string(id, rtn, htj->json_property_name, s_value);

    herr = H5Tclose(data_type);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not close data_type (s_value)\n", id);
      exit (-1);
    }

    herr = H5Tclose(mem_type);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not close mem_type (s_value)\n", id);
      exit (-1);
    }
    free(s_value);
    s_value = NULL;
    break;

  case 'F':
    // Here we assume we know the array length ahead of time.  Probably a bad assumption.

    data_space = H5Dget_space(data_set);
    if (data_space < 0) {
      fprintf(stderr, "%s: Could not get data_space (float array)\n", id);
      exit (-1);
    }

    rank = H5Sget_simple_extent_ndims(data_space);
    if (rank < 0) {
      fprintf(stderr, "%s: Could not get rank of data space (float array)\n", id);
      exit (-1);
    }
    
    dims = calloc(rank, sizeof(*dims));
    if (dims == NULL) {
      fprintf(stderr, "%s: Could not allocate memory for dims array (float array)\n", id);
      exit (-1);
    }

    herr = H5Sget_simple_extent_dims(data_space, dims, NULL);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not get dimensions of float array\n", id);
      exit (-1);
    }
      
    npoints = H5Sget_simple_extent_npoints(data_space);
    if (npoints < 0) {
      fprintf(stderr, "%s: Failed to get number of elements for float array\n", id);
      exit (-1);
    }

    fa_value = calloc( npoints, sizeof(float));
    if (fa_value == NULL) {
      fprintf(stderr, "%s: Out of memory (fa_value)\n", id);
      exit (-1);
    }

    herr = H5Dread(data_set, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, fa_value);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not read %d float values from %s\n", id, (int)npoints, htj->h5_location);
      exit (-1);
    }

    switch (rank) {
    case 1:
      set_json_object_float_array(id, rtn, htj->json_property_name, fa_value, dims[0]);
      break;
    case 2:
      set_json_object_float_array_2d(id, rtn, htj->json_property_name, fa_value, dims[1], dims[0]);
      break;

    default:
      fprintf(stderr, "%s: Unsupported json array rank (%d)\n", id, rank);
      exit (-1);
    }

    H5Sclose(data_space);
    free(dims);
    free(fa_value);
    fa_value = NULL;
    break;

  default:
    fprintf(stderr, "%s: data_set type code %c not implemented (%s)\n", id, htj->type, htj->h5_location);
    exit (-1);
  }
  herr = H5Dclose(data_set);
  if (herr < 0) {
    fprintf(stderr, "%s: could not close dataset for %s\n", id, htj->h5_location);
    exit (-1);
  }

  H5Dclose(data_set);
  return rtn;
}


void transmit_data_frames(void *skt, json_t *meta, frame_discovery_t *fp) {
  static const char *id = "transmit_data_frames";
  herr_t herr;
  hsize_t file_dims[3];
  hsize_t file_max_dims[3];
  hid_t mem_space;
  hsize_t mem_dims[2];
  hsize_t start[3];
  hsize_t stride[3];
  hsize_t count[3];
  hsize_t block[3];
  int rank;
  int data_element_size;
  char *compressed_buffer;
  int compressed_data_size;
  int k;
  int nframes;

  rank = H5Sget_simple_extent_ndims(fp->file_space);
  if (rank < 0) {
    fprintf(stderr, "%s: Failed to get rank of dataset\n", id);
    exit (-1);
  }

  if (rank != 3) {
    fprintf(stderr, "%s: Unexpected value of data_set rank.  Got %d but should gotten 3\n", id, rank);
    exit (-1);
  }

  herr = H5Sget_simple_extent_dims( fp->file_space, file_dims, file_max_dims);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not get dataset dimensions\n", id);
    exit (-1);
  }

  data_element_size = H5Tget_size( fp->file_type);
  if (data_element_size == 0) {
    fprintf(stderr, "%s: Could not get data_element_size\n", id);
    exit (-1);
  }

  if (data_element_size * file_dims[1] * file_dims[2] > IMAGE_SIZE) {
    fprintf(stderr, "%s: element size %d  image width %d  image height %d gives a size %d bytes which exceeds our maximum of %d bytes: Did you get a bigger detector?  You need to recompile this code.\n",
            id, data_element_size, (int)file_dims[1], (int)file_dims[2], (int)(data_element_size*file_dims[1]*file_dims[2]), IMAGE_SIZE);
    exit (-1);
  }

  mem_dims[0] = file_dims[1];
  mem_dims[1] = file_dims[2];
  mem_space = H5Screate_simple(2, mem_dims, mem_dims);
  if (mem_space < 0) {
    fprintf(stderr, "Could not create mem_space\n");
    exit (-1);
  }

  start[1] = 0;
  start[2] = 0;

  stride[0] = 1;
  stride[1] = 1;
  stride[2] = 1;

  count[0] = 1;
  count[1] = 1;
  count[2] = 1;

  block[0] = 1;
  block[1] = file_dims[1];
  block[2] = file_dims[2];


  nframes = fp->last_frame - fp->first_frame + 1;

  for (k=0; k < nframes; k++) {
    start[0] = k;

    herr = H5Sselect_hyperslab(fp->file_space, H5S_SELECT_SET, start, stride, count, block);
    if (herr < 0) {
      fprintf(stderr, "Could not set hyperslab for frame %d\n", fp->first_frame + k);
      exit (-1);
    }
    
    //
    // TODO: Check that the file space size is <= IMAGE_SIZE (ie, the data_buffer size);
    //

    herr = H5Dread(fp->data_set, fp->file_type, mem_space, fp->file_space, H5P_DEFAULT, data_buffer);
    if (herr < 0) {
      fprintf(stderr, "Could not read frame %d\n", fp->first_frame + k);
      exit (-1);
    }

    compressed_buffer = get_compressed_buffer();
    compressed_data_size = LZ4_compress_default(data_buffer, compressed_buffer, IMAGE_SIZE, COMPRESSED_BUFFER_SIZE);
    if (compressed_data_size == 0) {
      fprintf(stderr, "Failed to compress data for frame %d\n", fp->first_frame + k);
    }

    data_part1(skt, series, fp->first_frame + k, "unsupported");
    data_part2(skt, file_dims[1], file_dims[2], data_element_size == 2 ? "uint16" : "uint32", "lz4<", compressed_data_size);
    data_part3(skt, compressed_buffer, compressed_data_size);

    // Dectris does not store frame times (or any date information) in the Filewriter versions of the h5 files
    // TODO: store (and then retrieve here) the timestamps we retrived via the stream
    //
    data_part4(skt, 0, 0, 0);   
    data_appendix(skt, meta);
  }
}


    //
    // Expect parameters from parse JSON string from the ISREQESTS list
    //
    //  type:        String   The output type request.  JPEG for now.  TODO: movies, profiles, tarballs, whatever
    //  tag:         String   Identifies (with pid) the connection that needs this image
    //  pid:         String   Identifies the user who is requesting the image
    //  stop:        Boolean  Tells the output method to cease updating image
    //  fn:          String   Our file name.  Either relative or fully qualified OK
    //  frame:       Int      Our frame number
    //  xsize:       Int      Width of output image
    //  ysize:       Int      Height of output image
    //  contrast:    Int      greater than this is black.  -1 means self calibrate, whatever that means
    //  wval:        Int      less than this is white.     -1 means self calibrate, whatever that means
    //  x:           Int      X coord of original image to be left hand edge
    //  y:           Int      Y coord of original image to the the top edge
    //  width:       Int      Width in pixels on original image to process.  -1 means full width
    //  height:      Int      Height in pixels on original image to process.  -1 means full height
    //  labelHeight: Int      Height of the label to be placed at the top of the image (full output jpeg is ysize+labelHeight tall)
    //

typedef struct isRequestStruct {
  const char *type;
  const char *tag;
  const char *pid;
  int stop;
  const char *fn;
  int frame;
  int xsize;
  int ysize;
  int contrast;
  int wval;
  int x;
  int y;
  int width;
  int height;
  int labelHeight;
} isRequestType;

isRequestType *isRequestParser(json_t *obj) {
  isRequestType *rtn;

  rtn = calloc(1, sizeof(isRequestType));
  if (rtn == NULL) {
    fprintf(stderr, "Out of memory (isReqeustParser)\n");
    exit(-1);
  }
  rtn->type        = json_string_value(json_object_get(obj, "type"));
  rtn->tag         = json_string_value(json_object_get(obj, "tag"));
  rtn->pid         = json_string_value(json_object_get(obj, "pid"));
  rtn->stop        = json_is_true(json_object_get(obj, "stop"));
  rtn->fn          = json_string_value(json_object_get(obj, "fn"));
  rtn->frame       = json_integer_value(json_object_get(obj, "frame"));
  rtn->xsize       = json_integer_value(json_object_get(obj, "xsize"));
  rtn->ysize       = json_integer_value(json_object_get(obj, "ysize"));
  rtn->x           = json_integer_value(json_object_get(obj, "x"));
  rtn->y           = json_integer_value(json_object_get(obj, "y"));
  rtn->width       = json_integer_value(json_object_get(obj, "width"));
  rtn->height      = json_integer_value(json_object_get(obj, "height"));
  rtn->labelHeight =json_integer_value(json_object_get(obj, "labelHeight"));
}


int main(int argc, char **argv) {
  isRequestType *isRequest;
  herr_t herr;
  hid_t master_file;
  hid_t data_set;
  hid_t data_space;
  hsize_t dims[2];
  int rank;
  int npoints;
  double *flatfield;
  double *pixelmask;
  double *countrate_correction_table;
  const char *master_file_name;
  json_t *meta_obj;
  json_t *tmp_obj;
  json_t *master_path_obj;
  int i;
  frame_discovery_t *fp;
  int first_frame;
  int last_frame;
  int err;
  void *ctx;
  redisContext *rc;
  json_t *list_item_obj;
  json_error_t jerr;
  void *skt;
  redisReply *reply;

  //
  // setup redis
  //
  rc = redisConnect("10.1.253.10", 6379);
  if (rc == NULL || rc->err) {
    if (rc) {
      fprintf(stderr, "Failed to connect to redis: %s\n", rc->errstr);
    } else {
      fprintf(stderr, "Failed to get redis context\n");
    }
    exit (-1);
  }

  //
  // Here is our main loop
  //
  while (1) {
    //
    // Blocking request with no timeout.  We should be sitting here
    // patiently waiting for something to do.
    //
    reply = redisCommand(rc, "BRPOP ISREQUESTS 0");

    if (reply == NULL) {
      fprintf(stderr, "Redis error: %s\n", rc->errstr);
      exit (-1);
    }

    if (reply->type == REDIS_REPLY_ERROR) {
      fprintf(stderr, "Redis brpop command produced an error: %s\n", reply->str);
      exit (-1);
    }
  
    if (reply->type != REDIS_REPLY_STRING) {
      fprintf(stderr, "Redis brpop did not return a string, got type %d\n", reply->type);
      exit (-1);
    }

    list_item_obj = json_loads(reply->str, 0, &jerr);
    if (list_item_obj == NULL) {
      fprintf(stderr, "Failed to parse '%s': %s\n", reply->str, jerr.text);
      exit (-1);
    }
    freeReplyObject(reply);

    isRequest = isRequestParser(list_item_obj);

    //
    // Open up the master file
    //
    master_file = H5Fopen(isRequest->fn, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (master_file < 0) {
      fprintf(stderr, "Could not open master file %s\n", master_file_name);
      exit (-1);
    }

  
    //
    // Start with header part 1
    //
    header_part1(skt, series);
    
    //
    // Find the meta data
    //
    meta_obj = json_object();
    if (meta_obj == NULL) {
      fprintf(stderr, "Could not create metadata object\n");
      exit (-1);
    }

    for (i=0; i < sizeof(json_convert_array)/sizeof(json_convert_array[0]); i++) {
      tmp_obj = get_json(master_file, &json_convert_array[i]);
      err = json_object_update(meta_obj, tmp_obj);
      if (err != 0) {
        fprintf(stderr, "Could not update meta_obj\n");
        exit (-1);
      }
      json_decref(tmp_obj);
    }

  
    //
    // Now write out header part 2, the metadata
    //
    header_part2(skt, meta_obj);
    //
    // Still need meta_obj for the appendix: don't call json_decref yet.
    //

    //
    // Now lets mine the flat field correction.
    //
    data_set = H5Dopen2(master_file, "/entry/instrument/detector/detectorSpecific/flatfield", H5P_DEFAULT);
    if (data_set < 0) {
      fprintf(stderr, "Could not open flatfield dataset\n");
      exit (-1);
    }
    
    data_space = H5Dget_space(data_set);
    if (data_space < 0) {
      fprintf(stderr, "Could not get flatfield data_space\n");
      exit (-1);
    }
    
    rank = H5Sget_simple_extent_ndims(data_space);
    if (rank < 0) {
      fprintf(stderr, "Could not get flatfield rank\n");
      exit (-1);
    }
    
    if (rank != 2) {
      fprintf(stderr, "We do not know how to deal with a flatfield of rank %d.  Only rank 2 is supported\n", rank);
      exit (-1);
    }
    
    err = H5Sget_simple_extent_dims(data_space, dims, NULL);
    if (err < 0) {
      fprintf(stderr, "Could not get flatfield dimensions\n");
      exit (-1);
    }
    
    //
    // Send out the flatfield header
    //
    header_part3(skt, dims[1], dims[0]);
    
    npoints = H5Sget_simple_extent_npoints(data_space);
    if (npoints < 0) {
      fprintf(stderr, "Could not get flatfield npoints\n");
      exit (-1);
    }
    
    flatfield = calloc(npoints, sizeof(*flatfield));
    if (flatfield == NULL) {
      fprintf(stderr, "Could not allocated memory for the flatfield\n");
      exit (-1);
    }
    
    err = H5Dread(data_set, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, flatfield);
    if (err < 0) {
      fprintf(stderr, "Failed to read flatfield data\n");
      exit (-1);
    }
    
    //
    // Send out the flatfield
    //
    header_part4(skt, flatfield, npoints * sizeof(*flatfield));
    
    // zmq now owns the flatfield buffer and will free it using our_free
    // whenever it feels like it.
    //
    
    herr = H5Sclose(data_space);
    if (herr < 0) {
      fprintf(stderr, "Could not close flatfield data space\n");
      exit (-1);
    }
    
    herr = H5Dclose(data_set);
    if (herr < 0) {
      fprintf(stderr, "Could not close flatfield data set\n");
      exit (-1);
    }
    
    //
    // Now lets work on the pixel mask
    //
    data_set = H5Dopen2(master_file, "/entry/instrument/detector/detectorSpecific/pixel_mask", H5P_DEFAULT);
    if (data_set < 0) {
      fprintf(stderr, "Could not open pixel mask data set\n");
      exit (-1);
    }
    
    data_space = H5Dget_space(data_set);
    if (data_space < 0) {
      fprintf(stderr, "Could not open pixel mask data space\n");
      exit (-1);
    }
    
    rank = H5Sget_simple_extent_ndims(data_space);
    if (rank < 0) {
      fprintf(stderr, "Could not get pixel mask rank\n");
      exit (-1);
    }
    
    if (rank != 2) {
      fprintf(stderr, "We do not know how to deal with a pixel mask of rank %d.  It should be 2\n", rank);
      exit (-1);
    }
    
    err = H5Sget_simple_extent_dims(data_space, dims, NULL);
    if (err < 0) {
      fprintf(stderr, "Could not get pixelmask dimentions\n");
      exit (-1);
    }
    
    //
    // Send out the pixel mask header
    //
    header_part5(skt, dims[1], dims[0]);
    
    npoints = H5Sget_simple_extent_npoints(data_space);
    if (npoints < 0) {
      fprintf(stderr, "Could not get pixel mask dimensions\n");
      exit (-1);
    }
    
    pixelmask = calloc(npoints, sizeof(*pixelmask));
    if (pixelmask == NULL) {
      fprintf(stderr, "Could not allocate memory for the pixelmask\n");
      exit (-1);
    }
    
    
    err = H5Dread(data_set, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, pixelmask);
    if (err < 0) {
      fprintf(stderr, "Could not read pixelmask data\n");
      exit (-1);
    }
    
    //
    // Send out the pixel mask
    //
    header_part6(skt, pixelmask, npoints * sizeof(*pixelmask));
    
    // zmq will free pixelmask with our_free
    
    herr = H5Sclose(data_space);
    if (herr < 0) {
      fprintf(stderr, "Could not close pixelmask data space\n");
      exit (-1);
    }
    
    herr = H5Dclose(data_set);
    if (herr < 0) {
      fprintf(stderr, "Could not close pixelmask data set\n");
      exit (-1);
    }
    
    //
    // Now lets go for that count rate
    //
    data_set = H5Dopen2(master_file, "/entry/instrument/detector/detectorSpecific/detectorModule_000/countrate_correction_table", H5P_DEFAULT);
    if (data_set < 0) {
      fprintf(stderr, "Could not open countrate correction table in master_file\n");
      exit (-1);
    }
    
    data_space = H5Dget_space(data_set);
    if (data_space < 0) {
      fprintf(stderr, "Could not get countrate correction table data space\n");
      exit (-1);
    }
    
    rank = H5Sget_simple_extent_ndims(data_space);
    if (rank < 0) {
      fprintf(stderr, "Could not get countrate correction table rank\n");
      exit (-1);
    }
    
    if (rank != 2) {
      fprintf(stderr, "We do not know what a count rate correction table of rank %d even means.  Try again with rank 2\n", rank);
      exit (-1);
    }
    
    err = H5Sget_simple_extent_dims(data_space, dims, NULL);
    if (err < 0) {
      fprintf(stderr, "Could not get countrate correction table dimensions\n");
      exit (-1);
    }
    
    //
    // Now we know enough to send out the header
    //
    header_part7(skt, dims[0]);
    
    npoints = H5Sget_simple_extent_npoints(data_space);
    if (npoints < 0) {
      fprintf(stderr, "Could not get count rate correction table npoints\n");
      exit (-1);
    }
    
    countrate_correction_table = calloc(npoints, sizeof(*countrate_correction_table));
    if (countrate_correction_table == NULL) {
      fprintf(stderr, "Failed to allocate space for countrate correction table\n");
      exit (-1);
    }
    
    err = H5Dread(data_set, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, countrate_correction_table);
    if (err < 0) {
      fprintf(stderr, "Failed to read countrate correction table\n");
      exit (-1);
    }
    
    //
    // Send out countrate correction table
    //
    header_part8(skt, countrate_correction_table, npoints * sizeof(*countrate_correction_table));
    
    // countrate_correction will be freed by zmq
    
    herr = H5Sclose(data_space);
    if (herr < 0) {
      fprintf(stderr, "Could not close countrate correction table data space\n");
      exit (-1);
    }
    
    herr = H5Dclose(data_set);
    if (herr < 0) {
      fprintf(stderr, "Could not close countrate correction table data set\n");
      exit (-1);
    }
    
    //
    // Remember that meta data from so long ago?
    //
    tmp_obj = json_object_get(list_item_obj, "meta");
    if (tmp_obj == NULL) {
      fprintf(stderr, "Could not file meta entry in list_item_obj\n");
      exit (-1);
    }
    
    err = json_object_update(meta_obj, tmp_obj);
    if (err != 0) {
      fprintf(stderr, "Could not update meta_obj\n");
      exit (-1);
    }
  
    header_appendix(skt, meta_obj);
    //
    // Don't call json_decref on meta_obj yet: we still need it for
    // the data appendix.
    //


    //
    // At this point we are done with the 9 message parts that make up the header message.
    //
    // Now to deal with the data
    //


    //
    // Find which frame is where
    //
    herr = H5Lvisit_by_name(master_file, "/entry/data", H5_INDEX_NAME, H5_ITER_INC, discovery_cb, NULL, H5P_DEFAULT);
    if (herr < 0) {
      fprintf(stderr, "Could not discover which frame is where\n");
      exit (-1);
    }


    //
    // Find range of frame numbers
    //
    fp = frame_discovery_base;
    if (fp == NULL) {
      fprintf(stderr, "There do not seem to be any frames\n");
      exit (-1);
    }
    first_frame = fp->first_frame;
    last_frame  = fp->last_frame;
    
    for (fp = fp->next; fp != NULL; fp = fp->next) {
      if (fp->first_frame < first_frame) {
        first_frame = fp->first_frame;
      }
      if (fp->last_frame > last_frame) {
        last_frame = fp->last_frame;
      }
    }

    //
    // Write frames all the rest of the frames too
    //
    for (fp = frame_discovery_base; fp != NULL; fp = fp->next) {
      transmit_data_frames(skt, meta_obj, fp);
    }
    
    herr = H5Fclose(master_file);
    if (herr < 0) {
      fprintf(stderr, "Could not close master file\n");
      exit (-1);
    }
  }
  return 0;
}
