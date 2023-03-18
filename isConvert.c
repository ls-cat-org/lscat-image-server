#include <assert.h>
#include <stdint.h>
#include <jansson.h>
#include <hdf5.h>

#include "is.h"

// Get the software_version first so we can determine which params we have.
const struct h5_json_property json_convert_software_version = {
  "/entry/instrument/detector/detectorSpecific/software_version", "software_version"};

/**
 * Our mapping between hdf5 file properties and our metadata object properties for 
 * datasets produced by the Eiger 2X 16M, and all other v1.8 DCUs.
 */
const struct h5_json_property json_convert_array_1_8[] = {
  {"/entry/instrument/detector/detectorSpecific/software_version",                "software_version"},
  {"/entry/instrument/detector/detectorSpecific/auto_summation",                  "auto_summation"},
  {"/entry/instrument/detector/beam_center_x",                                    "beam_center_x"},
  {"/entry/instrument/detector/beam_center_y",                                    "beam_center_y"},
  {"/entry/instrument/detector/bit_depth_readout",                                "bit_depth_readout"},
  {"/entry/instrument/detector/bit_depth_image",                                  "bit_depth_image"},
  {"/entry/instrument/detector/count_time",                                       "count_time"},
  {"/entry/instrument/detector/detectorSpecific/data_collection_date",            "data_collection_date"},
  {"/entry/instrument/detector/description",                                      "description"},
  {"/entry/instrument/detector/detector_distance",                                "detector_distance"},
  {"/entry/instrument/detector/detector_number",                                  "detector_number"},
  {"/entry/instrument/detector/geometry/orientation/value",                       "detector_orientation"},
  {"/entry/instrument/detector/detector_readout_time",                            "detector_readout_time"},
  {"/entry/instrument/detector/geometry/translation/distances",                   "detector_translation"},
  {"/entry/instrument/detector/detectorSpecific/element",                         "element"},
  {"/entry/instrument/detector/flatfield_correction_applied",                     "flatfield_correction_applied"},
  {"/entry/instrument/detector/detectorSpecific/frame_count_time",                "frame_count_time"},
  {"/entry/instrument/detector/detectorSpecific/frame_period",                    "frame_period"},
  {"/entry/instrument/detector/frame_time",                                       "frame_time"},
  {"/entry/instrument/detector/detectorSpecific/nimages",                         "nimages"},
  {"/entry/instrument/detector/detectorSpecific/ntrigger",                        "ntrigger"},
  {"/entry/instrument/detector/detectorSpecific/number_of_excluded_pixels",       "number_of_excluded_pixels"},
  {"/entry/instrument/detector/detectorSpecific/photon_energy",                   "photon_energy"},
  {"/entry/instrument/detector/pixel_mask_applied",                               "pixel_mask_applied"},
  {"/entry/instrument/detector/sensor_material",                                  "sensor_material"},
  {"/entry/instrument/detector/sensor_thickness",                                 "sensor_thickness"},
  {"/entry/instrument/detector/threshold_energy",                                 "threshold_energy"},
  {"/entry/instrument/detector/detectorSpecific/trigger_mode",                    "trigger_mode"},
  {"/entry/instrument/detector/virtual_pixel_correction_applied",                 "virtual_pixel_correction_applied"},
  {"/entry/instrument/beam/incident_wavelength",                                  "wavelength"},
  {"/entry/instrument/detector/x_pixel_size",                                     "x_pixel_size"},
  {"/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",            "x_pixels_in_detector"},
  {"/entry/instrument/detector/y_pixel_size",                                     "y_pixel_size"},
  {"/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",            "y_pixels_in_detector"},

  // Params new to v1.8
  
  {"/entry/definition",                                                           "definition"},
  {"/entry/instrument/detector/detectorSpecific/type",                            "type"},
  {"/entry/instrument/detector/distance",                                         "distance"},
  {"/entry/instrument/detector/goniometer/two_theta",                             "two_theta"},
  {"/entry/instrument/detector/goniometer/two_theta_end",                         "two_theta_end"},
  {"/entry/instrument/detector/goniometer/two_theta_range_average",               "two_theta_range_average"},
  {"/entry/instrument/detector/goniometer/two_theta_range_total",                 "two_theta_range_total"},
  {"/entry/instrument/detector/saturation_value",                                 "saturation_value"},
  {"/entry/instrument/detector/transformations/translation",                      "translation"},
  
  {"/entry/sample/goniometer/omega",               "omega"},
  {"/entry/sample/goniometer/omega_end",           "omega_end"},
  {"/entry/sample/goniometer/omega_range_average", "omega_range_average"},
  {"/entry/sample/goniometer/omega_range_total",   "omega_range_total"},
  {"/entry/sample/goniometer/chi",                 "chi"},
  {"/entry/sample/goniometer/chi_end",             "chi_end"},
  {"/entry/sample/goniometer/chi_range_average",   "chi_range_average"},
  {"/entry/sample/goniometer/chi_range_total",     "chi_range_total"},
  {"/entry/sample/goniometer/phi",                 "phi"},
  {"/entry/sample/goniometer/phi_end",             "phi_end"},
  {"/entry/sample/goniometer/phi_range_average",   "phi_range_average"},
  {"/entry/sample/goniometer/phi_range_total",     "phi_range_total"},
  
  {"/entry/instrument/detector/detectorSpecific/countrate_correction_lookuptable", "countrate_correction_lookuptable"},
  {"/entry/instrument/detector/detectorSpecific/countrate_correction_table",       "countrate_correction_table"},
  {"/entry/instrument/detector/module/data_origin",                                "data_origin"},
  {"/entry/instrument/detector/module/data_size",                                  "data_size"},
  {"/entry/instrument/detector/module/fast_pixel_direction",                       "fast_pixel_direction"},
  {"/entry/instrument/detector/module/module_index",                               "module_index"},
  {"/entry/instrument/detector/module/module_offset",                              "module_offset"},
  {"/entry/instrument/detector/module/slow_pixel_direction",                       "slow_pixel_direction"}
};
const size_t json_convert_array_1_8_size = sizeof(json_convert_array_1_8)/sizeof(struct h5_json_property);

/**
 * Our mapping between hdf5 file properties and our metadata object properties for 
 * datasets produced by the Eiger 9M, and all other v1.6 DCUs.
 */
const struct h5_json_property json_convert_array_1_6[] = {
  {"/entry/instrument/detector/detectorSpecific/software_version",                 "software_version"},
  { "/entry/instrument/detector/detectorSpecific/auto_summation",                  "auto_summation"},
  { "/entry/instrument/detector/beam_center_x",                                    "beam_center_x"},
  { "/entry/instrument/detector/beam_center_y",                                    "beam_center_y"},
  { "/entry/instrument/detector/bit_depth_readout",                                "bit_depth_readout"},
  { "/entry/instrument/detector/bit_depth_image",                                  "bit_depth_image"},
  { "/entry/instrument/detector/detectorSpecific/calibration_type",                "calibration_type"},
  { "/entry/sample/goniometer/chi_increment",                                      "chi_increment"},
  { "/entry/sample/goniometer/chi_start",                                          "chi_start"},
  { "/entry/instrument/detector/count_time",                                       "count_time"},
  { "/entry/instrument/detector/detectorSpecific/countrate_correction_bunch_mode", "countrate_correction_bunch_mode"},
  { "/entry/instrument/detector/detectorSpecific/data_collection_date",            "data_collection_date"},
  { "/entry/instrument/detector/description",                                      "description"},
  { "/entry/instrument/detector/detector_distance",                                "detector_distance"},
  { "/entry/instrument/detector/detector_number",                                  "detector_number"},
  { "/entry/instrument/detector/geometry/orientation/value",                       "detector_orientation"},
  { "/entry/instrument/detector/detectorSpecific/detector_readout_period",         "detector_readout_period"},
  { "/entry/instrument/detector/detector_readout_time",                            "detector_readout_time"},
  { "/entry/instrument/detector/geometry/translation/distances",                   "detector_translation"},
  { "/entry/instrument/detector/efficiency_correction_applied",                    "efficiency_correction_applied"},
  { "/entry/instrument/detector/detectorSpecific/element",                         "element"},
  { "/entry/instrument/detector/flatfield_correction_applied",                     "flatfield_correction_applied"},
  { "/entry/instrument/detector/detectorSpecific/frame_count_time",                "frame_count_time"},
  { "/entry/instrument/detector/detectorSpecific/frame_period",                    "frame_period"},
  { "/entry/instrument/detector/frame_time",                                       "frame_time"},
  { "/entry/sample/goniometer/kappa_increment",                                    "kappa_increment"},
  { "/entry/sample/goniometer/kappa_start",                                        "kappa_start"},
  { "/entry/instrument/detector/detectorSpecific/nframes_sum",                     "nframes_sum"},
  { "/entry/instrument/detector/detectorSpecific/nimages",                         "nimages"},
  { "/entry/instrument/detector/detectorSpecific/ntrigger",                        "ntrigger"},
  { "/entry/instrument/detector/detectorSpecific/number_of_excluded_pixels",       "number_of_excluded_pixels"},
  { "/entry/sample/goniometer/omega_increment",                                    "omega_increment"},
  { "/entry/sample/goniometer/omega_start",                                        "omega_start"},
  { "/entry/sample/goniometer/phi_increment",                                      "phi_increment"},
  { "/entry/sample/goniometer/phi_start",                                          "phi_start"},
  { "/entry/instrument/detector/detectorSpecific/photon_energy",                   "photon_energy"},
  { "/entry/instrument/detector/pixel_mask_applied",                               "pixel_mask_applied"},
  { "/entry/instrument/detector/sensor_material",                                  "sensor_material"},
  { "/entry/instrument/detector/sensor_thickness",                                 "sensor_thickness"},
  { "/entry/instrument/detector/detectorSpecific/summation_nimages",               "summation_nimages"},
  { "/entry/instrument/detector/threshold_energy",                                 "threshold_energy"},
  { "/entry/instrument/detector/detectorSpecific/trigger_mode",                    "trigger_mode"},
  { "/entry/instrument/detector/goniometer/two_theta_increment",                   "two_theta_increment"},
  { "/entry/instrument/detector/goniometer/two_theta_start",                       "two_theta_start"},
  { "/entry/instrument/detector/virtual_pixel_correction_applied",                 "virtual_pixel_correction_applied"},
  { "/entry/instrument/beam/incident_wavelength",                                  "wavelength"},
  { "/entry/instrument/detector/x_pixel_size",                                     "x_pixel_size"},
  { "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",            "x_pixels_in_detector"},
  { "/entry/instrument/detector/y_pixel_size",                                     "y_pixel_size"},
  { "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",            "y_pixels_in_detector"}
};
const size_t json_convert_array_1_6_size = sizeof(json_convert_array_1_6)/sizeof(struct h5_json_property);

/**
 * 
 * @note { This struct and functions operating on it do not regard datasets 
 * of type string as arrays. }
 *
 * @todo { Only the following classes of datatype are supported:
 * H5T_INTEGER, H5T_FLOAT, H5T_STRING, and H5T_ARRAY (containing elements of 
 * type H5T_FLOAT or H5T_INTEGER types only). }
 */
struct h5_attributes {
  hid_t       dataset;
  bool        is_scalar; // True for primitive types, false for arrays and compound types.

  // If the dataset is an array, this is the base type of the individual
  // elements. This is done by calling H5Tget_super().
  hid_t       datatype;          // Handle used to access other fields below.
  size_t      datatype_size;     // For strings, the size (in bytes) of the string. Otherwise, the size of 1 element.
  H5T_class_t datatype_class;    // A less-specific "category of type".
  H5T_cset_t  datatype_cset;     // If the data is a string, this is the charset. Should be UTF-8.

  // If the dataset is a scalar or string, these fields are unpopulated.
  hid_t       dataspace;         // handle used to access the information below
  int         dataspace_ndims;   // tensor rank
  hsize_t*    dataspace_dims;    // tensor dimensions (hsize_t is long long unsigned int)
  hssize_t    dataspace_npoints; // total number of elements
  
};

static void h5_attributes_dtor(struct h5_attributes* attrs) {
  // No need to check error status on closing descriptors and freeing memory:
  // If there is a real resource that the underlying library fails to release it,
  // there is nothing we can do about it.
  if (attrs->dataspace_dims != NULL) free(attrs->dataspace_dims);
  if (attrs->dataspace >= 0) H5Sclose(attrs->dataspace);
  if (attrs->datatype  >= 0) H5Tclose(attrs->datatype);
  if (attrs->dataset   >= 0) H5Dclose(attrs->dataset);

  attrs->dataset           = -1;
  attrs->is_scalar         = false;
  attrs->datatype          = -1;
  attrs->datatype_size     = 0;
  attrs->datatype_class    = H5T_NO_CLASS;
  attrs->datatype_cset     = H5T_CSET_ERROR;
  attrs->dataspace         = -1;
  attrs->dataspace_ndims   = 0;
  attrs->dataspace_dims    = NULL;
  attrs->dataspace_npoints = 0;
}

static void h5_attributes_ctor(hid_t file, const char* h5_property, struct h5_attributes* out) {
#define ERRLOG(msg) isLogging_err("%s: id=%d %s - " msg "\n", log_id, (int)file, h5_property);
  static const char* log_id = FILEID "h5_attributes_ctor";
  int         errcode_int = 0;

  out->dataset           = -1;
  out->is_scalar         = false;
  out->datatype          = -1;
  out->datatype_size     = 0;
  out->datatype_class    = H5T_NO_CLASS;
  out->datatype_cset     = H5T_CSET_ERROR;
  out->dataspace         = -1;
  out->dataspace_ndims   = 0;
  out->dataspace_dims    = NULL;
  out->dataspace_npoints = 0;
  
  out->dataset = H5Dopen2(file, h5_property, H5P_DEFAULT);
  if (out->dataset < 0) {
    ERRLOG("H5DOpen2 failed");
    goto error;
  }

  out->datatype = H5Dget_type(out->dataset);
  if (out->datatype < 0) {
    ERRLOG("H5Dget_type failed");
    goto error;
  }
  
  out->datatype_class = H5Tget_class(out->datatype);
  if (out->datatype_class == H5T_NO_CLASS) {
    ERRLOG("H5Tget_class failed");
    goto error;
  }

  out->dataspace = H5Dget_space(out->dataset);
  if (out->dataspace < 0) {
    ERRLOG("H5Dget_space failed");
    goto error;
  }
  
  out->dataspace_ndims = H5Sget_simple_extent_ndims(out->dataspace);
  if (out->dataspace_ndims < 0) {
    ERRLOG("H5Dget_simple_extent_ndims failed");
    goto error;
  }
  out->dataspace_dims = malloc(out->dataspace_ndims*sizeof(int));
  
  errcode_int = H5Sget_simple_extent_dims(out->dataspace, out->dataspace_dims, /*maxdims*/NULL);
  if (errcode_int < 0) {
    ERRLOG("H5Sget_simple_extent_dims failed");
    goto error;
    
  } else if (errcode_int != out->dataspace_ndims) {
    isLogging_err("h5_attributes_ctor: FATAL ERROR, H5Sget_simple_extent_dims return value "
		  " disagrees with n_dims. Expected %d, got %d. Dataset: %s%s.",
		  out->dataspace_ndims, errcode_int, file, h5_property);
    goto error;
  }
  
  out->dataspace_npoints = H5Sget_simple_extent_npoints(out->dataspace);
  if (out->dataspace_npoints == 0) {
    ERRLOG("H5Sget_simple_extent_npoints failed");
    goto error;
  };
  
  if (out->datatype_class == H5T_STRING) {
    // Get charset
    out->datatype_cset = H5Tget_cset(out->datatype);
    if (out->datatype_cset == H5T_CSET_ERROR) {
      ERRLOG("H5Tget_cset failed");
      goto error;
    } else if (out->datatype_cset != H5T_CSET_ASCII) {
      ERRLOG("H5Tget_cset returned an unsupported charset");
      goto error;
    }
  }

  // For strings, this is the size (in bytes) of the string.
  // For scalars, this is the size (in bytes) of the single number contained.
  // For arrays, this is the size (in bytes) of 1 element (i.e. 1 number).
  out->datatype_size = H5Tget_size(out->datatype);
  if (out->datatype_size <= 0) {
    isLogging_err("h5_attributes_ctor: H5Tget_size failed %s%s\n", file, h5_property);
    goto error;
  }

  out->is_scalar = (out->dataspace_npoints <= 1);
  return;
  
 error:
  h5_attributes_dtor(out);
  return;
#undef ERRLOG
}

// Take in-memory values stored in data, and write them to a (non-jagged) n-dimensional json array.
static json_t* tensor_to_json_helper(bool integer_type, size_t width, int ndims, hsize_t dims[], char* data) {
#define ERRLOG(msg) isLogging_err("%s: " msg "\n", log_id);
  static const char* log_id = FILEID "tensor_to_json_helper";
  json_t* result   = NULL;
  json_t* sub      = NULL; // sub-tensor for ndims > 1, otherwise an integer or real
  int     json_err = -1;
  size_t  subdimension_bytes = width;
  
  if (ndims > 1) {  // A higher-order tensor that must be recursed
    result = json_array();
    if (result == NULL) {
      ERRLOG("json_array() failed for tensor");
      goto error;
    }

    // Get the number of bytes to skip ahead to in the data buffer
    for (int i=1; i < ndims; ++i) {
      subdimension_bytes *= (size_t)dims[i];
    }
    
    for (int i=0; i < dims[0]; ++i) {
      sub = tensor_to_json_helper(integer_type, width, ndims-1, &dims[1],
				  &data[i*subdimension_bytes]);
      if (sub == NULL) {
	ERRLOG("failed to get sub-tensor");
	goto error;
      }

      // steals reference to sub, no need to json_decref()
      json_err = json_array_append_new(result, sub);
      if (json_err != 0) {
	ERRLOG("failed to append sub-tensor to array");
	goto error;
      }
    }
    
  } else if (ndims == 1) { // A vector (i.e. simple array)
    // 1D array? Iterate through elements
    result = json_array();
    if (result == NULL) {
      ERRLOG("json_array() failed for simple array");
      goto error;
    }

    for (int i=0; i < dims[0]; ++i) {
      // This mess of parentheses and pointer arithmetic is
      // necessary to get the proper data out.
      sub = integer_type ? json_integer( *( (json_int_t*)(&data[i*width]) ) ) :
	json_real( *( (double*)(&data[i*width]) ) );
      
      if (sub == NULL) {
	ERRLOG("failed to get element");
	goto error;
      }

      // steals referrence to sub, no need to json_decref()
      json_err = json_array_append_new(result, sub);
      if (json_err != 0) {
	ERRLOG("failed to append element to array");
	goto error;
      }
    }
    
  } else { // (hopefully) unreachable code
    assert(false && "bad logic in tensor_to_json");
  }
  
  return result;
  
 error:
  if (sub    != NULL) json_decref(sub);
  if (result != NULL) json_decref(result);
  return NULL;
#undef ERRLOG
}

// Convert an n-dimensional tensor of 8-byte numbers into a nested JSON array.
static json_t* tensor_to_json(const struct h5_attributes* attributes) {
#define ERRLOG(msg) isLogging_err("%s: " msg "\n", log_id);
  static const char* log_id = FILEID "tensor_to_json";
  json_t* result   = NULL;
  char*   data_buf = NULL;
  herr_t  h5_err   = -1;
  hid_t   memtype  = (attributes->datatype_class == H5T_INTEGER) ?
    H5Tcopy(H5T_NATIVE_INT64) : H5Tcopy(H5T_NATIVE_DOUBLE);
  if (memtype < 0) {
    ERRLOG("failed to initialize H5 type for memory buffer.");
    goto error;
  }
  
  data_buf = calloc(attributes->dataspace_npoints, 8);
  if (data_buf == NULL) {
    ERRLOG("out of memory");
    goto error;
  }
  
  h5_err = H5Dread(attributes->dataset, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, (void*)data_buf);
  if (h5_err < 0) {
    ERRLOG("failed to read dataset into memory");
    goto error;
  }

  result = tensor_to_json_helper((attributes->datatype_class == H5T_INTEGER), attributes->datatype_size,
				 attributes->dataspace_ndims, attributes->dataspace_dims, data_buf);
  
  H5Tclose(memtype);
  free(data_buf);
  return result;
  
 error:
  if (memtype >= 0) H5Tclose(memtype);
  if (data_buf != NULL) free(data_buf);
  return NULL;
#undef ERRLOG
}

json_t* string_to_json(const struct h5_attributes* attributes) {
#define ERRLOG(msg) isLogging_err("%s: " msg "\n", log_id);
  static const char* log_id = FILEID "string_to_json";
  json_t* result = NULL;
  char*   string_buf = NULL;
  size_t  string_buf_size  = 0;
  herr_t  h5_err  = 0; // h5 equivalent to errno, minus a useful strerror()
  hid_t   memtype = H5Tcopy(H5T_C_S1);
  if (memtype < 0) {
    ERRLOG("failed to initialize memtype");
    goto error;
  }
  
  h5_err = H5Tset_size(memtype, attributes->datatype_size);
  if (h5_err < 0) {
    ERRLOG("failed to set size for memtype");
    goto error;
  }
  
  string_buf_size = attributes->datatype_size + 1; // +1 for null-terminator.
  string_buf = (char*)calloc(string_buf_size, 1);
  if (string_buf == NULL) {
    isLogging_crit("string_to_json: - out of memory, cannot read string data.\n");
    goto error;
  }
  
  h5_err = H5Dread(attributes->dataset, memtype,
		   /*mem_space_id*/H5S_ALL, /*file_space_id*/H5S_ALL,
		   /*xfer_plist_id*/H5P_DEFAULT, string_buf);
  if (h5_err < 0) {
    ERRLOG("H5Dread failed for string");
    goto error;
  }
  
  result = json_string(string_buf);
  if (result == NULL) {
    ERRLOG("json_string failed");
    goto error;
  }

  H5Tclose(memtype);
  free(string_buf);
  return result;
  
 error:
  if (memtype >= 0)       H5Tclose(memtype);
  if (string_buf != NULL) free(string_buf);
  return NULL;
#undef ERRLOG
}

json_t* h5_property_to_json(hid_t file, const struct h5_json_property* property) {
#define ERRLOG(msg) isLogging_err("%s: id=%d %s - " msg "\n", log_id, (int)file, property->h5_location);
  static const char* log_id = FILEID "h5_to_json";
  struct h5_attributes h5_attrs;

  hid_t   memtype = -1;
  int64_t int64_value = 0;
  double  float64_value = 0;
  herr_t  h5_err  = 0; // h5 equivalent to errno, minus a useful strerror()

  json_t* json_value = NULL;
  int     json_err = 0;

  json_t* result = json_object();
  if (result == NULL) {
    ERRLOG("failed to initialize JSON object");
    goto error;
  }

  h5_attributes_ctor(file, property->h5_location, &h5_attrs);
  if (h5_attrs.dataset < 0) {
    ERRLOG("h5_attributes_ctor failed");
  }
  
  switch (h5_attrs.datatype_class) {
  case H5T_NO_CLASS:
    ERRLOG("h5_attributes_ctor failed");
    goto error;
    
  case H5T_INTEGER:
    memtype = H5Tcopy(H5T_NATIVE_INT64);
    if (memtype < 0) {
      ERRLOG("failed to initialize memtype for integer scalar");
      goto error;
    }
    
    if (h5_attrs.is_scalar) {
      h5_err = H5Dread(h5_attrs.dataset, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, &int64_value);
      if (h5_err < 0) {
	ERRLOG("H5Dread failed for integer scalar");
	goto error;
      }
      
      json_value = json_integer((json_int_t)int64_value);
      if (json_value == NULL) {
	ERRLOG("json_integer failed");
	goto error;
      }
    } else {
      json_value = tensor_to_json(&h5_attrs);
      if (json_value == NULL) {
	ERRLOG("tensor_to_json failed for integer array");
	goto error;
      }
    }
    H5Tclose(memtype);
    break;
    
  case H5T_FLOAT:
    memtype = H5Tcopy(H5T_NATIVE_DOUBLE);
    if (memtype < 0) {
      ERRLOG("failed to initialize memtype for real scalar");
      goto error;
    }
    
    if (h5_attrs.is_scalar) {
      h5_err = H5Dread(h5_attrs.dataset, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, &float64_value);
      if (h5_err < 0) {
	ERRLOG("H5Dread failed for real scalar");
	goto error;
      }
      
      json_value = json_real(float64_value);
      if (json_value == NULL) {
	ERRLOG("json_real failed");
	goto error;
      }
    } else {
      json_value = tensor_to_json(&h5_attrs);
      if (json_value == NULL) {
	ERRLOG("tensor_to_json failed for real array");
	goto error;
      }
    }
    H5Tclose(memtype);
    break;
    
  case H5T_STRING:
    json_value = string_to_json(&h5_attrs);
    if (json_value == NULL) {
      ERRLOG("string_to_json failed");
      goto error;
    }
    break;
    
  default:
    ERRLOG("unsupported data type");
    goto error;
  }

  json_err = json_object_set_new(result, property->json_name, json_value);
  if (json_err != 0) {
    ERRLOG("json_object_set_new failed for string");
    goto error;
  }
  
  h5_attributes_dtor(&h5_attrs);
  return result;
  
 error:
  if (memtype >=0) H5Tclose(memtype);
  h5_attributes_dtor(&h5_attrs);
  return NULL;
#undef ERRLOG
}

json_t* get_dcu_version(hid_t file) {
  return h5_property_to_json(file, &json_convert_software_version);
}
