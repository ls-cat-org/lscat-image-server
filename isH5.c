#include "is.h"

typedef struct frame_discovery_struct {
  struct frame_discovery_struct *next;
  hid_t data_set;
  hid_t file_space;
  hid_t file_type;
  int32_t first_frame;
  int32_t last_frame;
  char *done_list;
} frame_discovery_t;

typedef struct isH5extraStruct {
  frame_discovery_t *frame_discovery_base;
  void *bad_pixel_map;
} isH5extra_t;

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
  { "/entry/instrument/detector/bit_depth_image",                                  "bit_depth_image",                                 'i'},
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

json_t *get_json( hid_t master_file, h5_to_json_t *htj) {
  static const char *id = FILEID "get_json";
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

  return rtn;
}

json_t *isH5GetMeta(const char *fn) {
  static const char *id = FILEID "isH5Jpeg";
  hid_t master_file;
  herr_t herr;
  json_t *meta;
  json_t *tmp_obj;
  int i;
  int err;

  fprintf(stderr, "%s: trying to open file %s\n", id, fn);
  //
  // Open up the master file
  //
  master_file = H5Fopen(fn, H5F_ACC_RDONLY, H5P_DEFAULT);
  if (master_file < 0) {
    fprintf(stderr, "%s: Could not open master file %s\n", id, fn);
    return NULL;
  }

  //
  // Find the meta data
  //
  meta = json_object();
  if (meta == NULL) {
    fprintf(stderr, "%s: Could not create metadata object in file %s\n", id, fn);

    herr = H5Fclose(master_file);
    if (herr < 0) {
      fprintf(stderr, "%s: failed to close master file\n", id);
    }
    return NULL;
  }

  for (i=0; i < sizeof(json_convert_array)/sizeof(json_convert_array[0]); i++) {
    tmp_obj = get_json(master_file, &json_convert_array[i]);
    err = json_object_update(meta, tmp_obj);
    if (err != 0) {
      fprintf(stderr, "%s: Could not update meta_obj\n", id);
      
      herr = H5Fclose(master_file);
      if (herr < 0) {
        fprintf(stderr, "%s: failed to close master file\n", id);
      }
      return NULL;
    }
    json_decref(tmp_obj);
  }

  set_json_object_integer(id, meta, "image_depth",  json_integer_value(json_object_get(meta,"bit_depth_image"))/8);

  herr = H5Fclose(master_file);
  if (herr < 0) {
    fprintf(stderr, "%s: failed to close master file\n", id);
  }

  fprintf(stderr, "%s: returning with metadata\n", id);
  return meta;
}

int discovery_cb(hid_t lid, const char *name, const H5L_info_t *info, void *op_data) {
  static const char *id = FILEID "discovery_cb";
  isH5extra_t *extra;

  char s[256];
  const char *fnp;
  const char *pp;
  herr_t herr;
  hid_t image_nr_high;
  hid_t image_nr_low;
  frame_discovery_t *these_frames, *fp, *fpp;

  extra = op_data;

  these_frames = calloc(sizeof(frame_discovery_t), 1);
  if (these_frames == NULL) {
    fprintf(stderr, "%s: Out of memory (these_frames)\n", id);
    exit (-1);
  }

  //
  // setting next is more trouble than normal since we want to keep
  // the data in the same order that we were called.
  //

  fpp = NULL;
  for (fp = extra->frame_discovery_base; fp != NULL; fp = fp->next) {
    fpp = fp;
  }

  these_frames->next = NULL;
  if (fpp == NULL) {
    extra->frame_discovery_base = these_frames;
  } else {
    fpp->next = these_frames;
  }

  if (info->type == H5L_TYPE_EXTERNAL) {
    herr = H5Lget_val(lid, name, s, sizeof(s), H5P_DEFAULT);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not get link value %s\n", id, name);
      exit (-1);
    }

    herr = H5Lunpack_elink_val(s, sizeof(s), 0, &fnp, &pp);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not unpack link value for %s\n", id, name);
      exit (-1);
    }    
  }
  
  these_frames->data_set = H5Dopen2(lid, name, H5P_DEFAULT);
  if (these_frames->data_set < 0) {
    fprintf(stderr, "%s: Failed to open dataset %s\n", id, name);
    exit (-1);
  }

  these_frames->file_space = H5Dget_space(these_frames->data_set);
  if (these_frames->file_space < 0) {
    fprintf(stderr, "%s: Could not get data_set space for %s\n", id, name);
    exit (-1);
  }

  these_frames->file_type = H5Dget_type(these_frames->data_set);
  if (these_frames->file_type < 0) {
    fprintf(stderr, "%s: Could not get data_set type for %s\n", id, name);
    exit (-1);
  }

  image_nr_high = H5Aopen_by_name( lid, name, "image_nr_high", H5P_DEFAULT, H5P_DEFAULT);
  if (image_nr_high < 0) {
    fprintf(stderr, "%s: Could not open attribute 'image_nr_high' in linked file %s\n", id, name);
    exit (-1);
  }
  
  herr = H5Aread(image_nr_high, H5T_NATIVE_INT, &(these_frames->last_frame));
  if (herr < 0) {
    fprintf(stderr, "%s: Could not read value 'image_nr_high' in linked file %s\n", id, name);
    exit (-1);
  }

  herr = H5Aclose(image_nr_high);
  if (herr < 0) {
    fprintf(stderr, "%s: Failed to close attribute image_nr_high\n", id);
    exit (-1);
  }

  image_nr_low = H5Aopen_by_name( lid, name, "image_nr_low", H5P_DEFAULT, H5P_DEFAULT);
  if (image_nr_low < 0) {
    fprintf(stderr, "%s: Could not open attribute 'image_nr_low' in linked file %s\n", id, name);
    exit (-1);
  }
  
  herr = H5Aread(image_nr_low, H5T_NATIVE_INT, &(these_frames->first_frame));
  if (herr < 0) {
    fprintf(stderr, "%s: Could not read value 'image_nr_low' in linked file %s\n", id, name);
    exit (-1);
  }

  herr = H5Aclose(image_nr_low);
  if (herr < 0) {
    fprintf(stderr, "%s: Failed to close attribute image_nr_low\n", id);
    exit (-1);
  }

  these_frames->done_list = calloc( these_frames->last_frame - these_frames->first_frame + 1, 1);
  if (these_frames->done_list == NULL) {
    fprintf(stderr, "%s: Out of memory (done_list)\n", id);
    exit (-1);
  }

  return 0;
}

void get_one_frame(const char *fn, int frame, isImageBufType *imb) {
  static const char *id = FILEID "get_one_frame";
  isH5extra_t *extra;
  frame_discovery_t *fp;
  int rank;
  herr_t herr;
  hsize_t file_dims[3];
  hsize_t file_max_dims[3];
  int data_element_size;
  char *data_buffer;
  int   data_buffer_size;
  hid_t mem_space;
  hsize_t mem_dims[2];
  hsize_t start[3];
  hsize_t stride[3];
  hsize_t count[3];
  hsize_t block[3];

  extra = imb->extra;

  fprintf(stderr, "%s: start %s\n", id, extra==NULL ? "extra is null" : "");
  for (fp = extra->frame_discovery_base; fp != NULL; fp = fp->next) {
    fprintf(stderr, "%s: first_frame=%d  last_frame=%d\n", id, fp->first_frame, fp->last_frame);
    if (fp->first_frame <= frame && fp->last_frame >= frame) {
      break;
    }
  }
  if (fp == NULL) {
    fprintf(stderr, "%s: Could not file frame %d in file %s\n", id, frame, fn);
    return;
  }

  fprintf(stderr, "%s: 20\n", id);
  rank = H5Sget_simple_extent_ndims(fp->file_space);
  if (rank < 0) {
    fprintf(stderr, "%s: Failed to get rank of dataset for file %s\n", id, fn);
    return;
  }

  fprintf(stderr, "%s: 30\n", id);
  if (rank != 3) {
    fprintf(stderr, "%s: Unexpected value of data_set rank.  Got %d but should gotten 3\n", id, rank);
    return;
  }

  fprintf(stderr, "%s: 40\n", id);
  herr = H5Sget_simple_extent_dims( fp->file_space, file_dims, file_max_dims);
  if (herr < 0) {
    fprintf(stderr, "Could not get dataset dimensions\n");
    exit (-1);
  }

  fprintf(stderr, "%s: 50\n", id);
  data_element_size = H5Tget_size( fp->file_type);
  if (data_element_size == 0) {
    fprintf(stderr, "%s: Could not get data_element_size\n", id);
    return;
  }

  fprintf(stderr, "%s: 60\n", id);
  switch(data_element_size) {
  case 4:
    data_buffer_size = file_dims[1] * file_dims[2] * sizeof(uint32_t);
    break;
  case 2:
    data_buffer_size = file_dims[1] * file_dims[2] * sizeof(uint16_t);
    break;

  default:
    fprintf(stderr, "%s: Bad data element size, received %d instead of 2 or 4\n", id, data_element_size);
    return;
  }

  fprintf(stderr, "%s: About to allocate %d bytes\n", id, data_buffer_size);
  data_buffer = calloc(data_buffer_size, 1);
  if (data_buffer == NULL) {
    fprintf(stderr, "%s: Out of memory (data_buffer)\n", id);
    exit (-1);
  }

  mem_dims[0] = file_dims[1];
  mem_dims[1] = file_dims[2];
  mem_space = H5Screate_simple(2, mem_dims, mem_dims);
  if (mem_space < 0) {
    fprintf(stderr, "%s: Could not create mem_space\n", id);
    return;
  }

  start[0] = frame-1;
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

  fprintf(stderr, "%s: About to select hyperslab\n", id);
  herr = H5Sselect_hyperslab(fp->file_space, H5S_SELECT_SET, start, stride, count, block);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not set hyperslab for frame %d\n", id, frame);
    return;
  }
    
  fprintf(stderr, "%s: About to read data\n", id);
  herr = H5Dread(fp->data_set, fp->file_type, mem_space, fp->file_space, H5P_DEFAULT, data_buffer);
  if (herr < 0) {
    fprintf(stderr, "%s: Could not read frame %d\n", id, frame);
    return;
  }
  fprintf(stderr, "%s: Have finished reading data\n", id);

  imb->buf = data_buffer;
  imb->buf_size   = data_buffer_size;
  imb->buf_height = file_dims[1];
  imb->buf_width  = file_dims[2];
  imb->buf_depth  = data_element_size;

  fprintf(stderr, "%s: Done\n", id);
  return;
}


void isH5GetData(const char *fn, int frame, isImageBufType *imb) {
  static const char *id = FILEID "isH5GetData";
  isH5extra_t *extra;
  hid_t master_file;
  herr_t herr;
  
  fprintf(stderr, "%s: enter with fn='%s' frame=%d\n", id, fn, frame);
  extra = imb->extra;

  //
  // Open up the master file
  //
  master_file = H5Fopen(fn, H5F_ACC_RDONLY, H5P_DEFAULT);
  if (master_file < 0) {
    fprintf(stderr, "%s: Could not open master file %s\n", id, fn);
    return;
  }

  if (extra == NULL) {
    extra = calloc(1, sizeof(*(extra)));
    if (extra == NULL) {
      fprintf(stderr, "%s: Out of memory (extra)\n", id);
      exit(-1);
    }
    extra->frame_discovery_base = NULL;
    extra->bad_pixel_map = NULL;

    imb->extra = extra;
    //
    // Find which frame is where
    //
    herr = H5Lvisit_by_name(master_file, "/entry/data", H5_INDEX_NAME, H5_ITER_INC, discovery_cb, extra, H5P_DEFAULT);
    if (herr < 0) {
      fprintf(stderr, "%s: Could not discover which frame is where for file %s\n", id, fn);
      return;
    }
  }
  
  get_one_frame(fn, frame, imb);
  
  fprintf(stderr, "%s: done\n", id);
}
