#include <stdbool.h>
#include <bsd/string.h> // strlcopy, strlcat
#include <libgen.h>
#include <cbflib/cbf.h>
#include <cbflib/cbf_simple.h>
#include "is.h"

// The out parameters for cbf_get_integerarrayparameters_wdims_fs()
// Note: by default, each pixel is an unsigned 4-byte integer.
struct cbf_dims {
  unsigned int compression;
  int binary_id;   // (unused) index into start of CBF's binary data
  size_t elsize;   // bytes per pixel
  int elsigned;    // pixels are signed ints
  int elunsigned;  // pixels are unsigned ints
  size_t elements; // number of pixels
  int minelement;  // index of first pixel
  int maxelement;  // index of last pixel
  const char* byteorder; // 'big_endian' or 'little_endian'
  size_t dimfast;  // width
  size_t dimmid;   // height
  size_t dimslow;  // (unused)
  size_t padding;  // size of post-data padding
};

// Fields populated by various CBF getter functions.
// The value is retrieved by the libcbf function call without (prior to)
// any conversion of units into anything clients of the image server expect.
struct lscat_cbf_meta {
  double detector_distance; // TODO: What unit? Assuming mm for now.
  double photon_energy;    // Calculated from wavelength.
  double wavelength;       // unit: angstroms
  double omega_start;      // unit: degrees
  double omega_increment;  // unit: degrees
  double beam_center_x;    // unit: pixels
  double beam_center_y;    // unit: pixels
  double x_pixel_size;     // unit: mm
  double y_pixel_size;     // unit: mm
  double integration_time; // unit: seconds
};

/*
  param[in] errcode: libcbf error code bitmask (may contain multiple error codes)
  param[in] id: the name of the calling function
  param[in] cbf_func: the name of the libcbf function that returned an error code
  
  Error descriptions used below are taken verbatim from the libcbf documentation at:
  https://www.iucr.org/__data/iucr/cif/software/cbflib/CBFlib_0.7.9/doc/CBFlib.html#2.1.4
*/
static void log_cbf_error(int errcode, const char* id, const char* cbf_func) {
  assert(errcode != 0 && "log_cbf_error was called, but there is no error");
  if (errcode & CBF_FORMAT) {
    isLogging_err("%s: %s failed, \"The file format is invalid.\"\n", id, cbf_func);
  }

  if (errcode & CBF_ALLOC) {
    isLogging_err("%s: %s failed, \"Memory allocation failed.\"\n", id, cbf_func);
  }

  if (errcode & CBF_ARGUMENT) {
    isLogging_err("%s: %s failed, \"Invalid function argument.\"\n", id, cbf_func);
  }

  if (errcode & CBF_ASCII) {
    isLogging_err("%s: %s failed, \"The value is ASCII (not binary).\"\n", id, cbf_func);
  }

  if (errcode & CBF_BINARY) {
    isLogging_err("%s: %s failed, \"The value is binary (not ASCII).\"\n", id, cbf_func);
  }

  if (errcode & CBF_BITCOUNT) {
    isLogging_err("%s: %s failed, \"The expected number of bits does not match the actual number written.\"\n", id, cbf_func);
  }

  if (errcode & CBF_ENDOFDATA) {
    isLogging_err("%s: %s failed, \"The end of the data was reached before the end of the array.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILECLOSE) {
    isLogging_err("%s: %s failed, \"File close error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILEOPEN) {
    isLogging_err("%s: %s failed, \"File open error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILEREAD) {
    isLogging_err("%s: %s failed, \"File read error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILESEEK) {
    isLogging_err("%s: %s failed, \"File seek error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILETELL) {
    isLogging_err("%s: %s failed, \"File tell error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_FILEWRITE) {
    isLogging_err("%s: %s failed, \"File write error.\"\n", id, cbf_func);
  }

  if (errcode & CBF_IDENTICAL) {
    isLogging_err("%s: %s failed, \"A data block with the new name already exists.\"\n", id, cbf_func);
  }

  if (errcode & CBF_NOTFOUND) {
    isLogging_err("%s: %s failed, \"The data block, category, column or row does not exist.\"\n", id, cbf_func);
  }

  if (errcode & CBF_OVERFLOW) {
    isLogging_err("%s: %s failed, \"The number read cannot fit into the destination argument. The destination has been set to the nearest value.\"\n", id, cbf_func);
  }

  if (errcode & CBF_UNDEFINED) {
    isLogging_err("%s: %s failed, \"The requested number is not defined.\"\n", id, cbf_func);
  }

  if (errcode & CBF_NOTIMPLEMENTED) {
    isLogging_err("%s: %s failed, \"The requested functionality is not yet implemented.\"\n", id, cbf_func);
  }
}

/**
 * Retrieve only the meta data from a CBF image file,
 * and pass it back to the caller in the same format
 * as isRayonixGetMeta.
 *
 * @param[in] fn Our file name
 * @return JSON object containing metadata from a CBF file.
 */
json_t* isCbfGetMeta(const char *fn) {
  static const char *id = "isCbfGetMeta";
  json_t* rtn = NULL;
  int errcode = 0;
  FILE* f = NULL;
  cbf_handle cbf = 0;
  cbf_detector detr = 0;
  cbf_goniometer goni = 0;

  const int fn_buflen = strlen(fn) + 1;
  char* fn_basename = alloca(fn_buflen);
  strlcpy(fn_basename, fn, fn_buflen);
  fn_basename = basename(fn_basename);

  isLogging_info("%s: %s\n", id, fn);
  rtn = json_object();
  if (rtn == NULL) {
    isLogging_err("%s: Could not create return JSON object\n", id);
    goto error_return;
  }

  f = fopen(fn, "rb");
  if (f == NULL) {
    isLogging_err("%s: fopen failed to open CBF image file '%s'\n", id, fn);
    goto error_return;
  }

  errcode = cbf_make_handle(&cbf);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_make_handle");
    goto error_return;
  }

  errcode = cbf_read_widefile(cbf, f, MSG_NODIGEST);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_read_widefile");
    goto error_return;
  }
  f = NULL; // libcbf now owns the file handle

  struct lscat_cbf_meta meta;
  memset(&meta, 0, sizeof(struct lscat_cbf_meta));

  errcode = cbf_require_reference_detector(cbf, &detr, 0);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_require_reference_detector");
    meta.beam_center_x = 0;
    meta.beam_center_y = 0;
  } else {
    cbf_get_detector_distance(detr, &(meta.detector_distance));
    cbf_get_beam_center_fs(detr, &(meta.beam_center_x), &(meta.beam_center_y), NULL, NULL);
  }

  errcode = cbf_construct_goniometer(cbf, &goni);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_construct_goniometer");
    meta.omega_start = 0;
    meta.omega_increment = 0;
  } else {
    cbf_get_rotation_range(goni, 0, &(meta.omega_start), &(meta.omega_increment));
  }

  struct cbf_dims dims;
  memset(&dims, 0, sizeof(struct cbf_dims));
  cbf_get_integerarrayparameters_wdims_fs(cbf, &(dims.compression), &(dims.binary_id), &(dims.elsize),
					  &(dims.elsigned), &(dims.elunsigned), &(dims.elements),
					  &(dims.minelement), &(dims.maxelement), &(dims.byteorder),
					  &(dims.dimfast), &(dims.dimmid), &(dims.dimslow), &(dims.padding));
  
  cbf_get_pixel_size_fs(cbf, 0, 0, &(meta.x_pixel_size));
  cbf_get_pixel_size_fs(cbf, 0, 1, &(meta.y_pixel_size));
  cbf_get_wavelength(cbf, &(meta.wavelength));
  cbf_get_integration_time(cbf, 0, &(meta.integration_time));
  {
    set_json_object_string(id, rtn, "fn", fn);
    set_json_object_string(id, rtn, "filename", fn_basename);
    set_json_object_string(id, rtn, "filepath", fn);
    set_json_object_string(id, rtn, "comment",  "LS-CAT APS Sector 21");
    
    // TODO: Get this from the CBF.
    set_json_object_string(id, rtn, "detector", "PILATUS3X 6M");

    set_json_object_integer(id, rtn, "image_depth", (int)(dims.elsize));
    set_json_object_integer(id, rtn, "x_pixels_in_detector", (int)(dims.dimfast));
    set_json_object_integer(id, rtn, "y_pixels_in_detector", (int)(dims.dimmid));

    // Mandatory information.
    set_json_object_real(id, rtn, "detector_distance", (meta.detector_distance)/1000.0); // mm to m
    set_json_object_real(id, rtn, "rotationRange",     meta.omega_increment); // degrees
    set_json_object_real(id, rtn, "startPhi",          0.0); // degrees, note: samples only move along omega axis
    set_json_object_real(id, rtn, "photon_energy",     12398.4193 / (meta.wavelength));
    set_json_object_real(id, rtn, "wavelength",        meta.wavelength); // angstroms
    set_json_object_real(id, rtn, "beam_center_x",     meta.beam_center_x); // pixels
    set_json_object_real(id, rtn, "beam_center_y",     meta.beam_center_y); // pixels
    set_json_object_real(id, rtn, "x_pixel_size",      (meta.x_pixel_size)/1000); // mm to m
    set_json_object_real(id, rtn, "y_pixel_size",      (meta.y_pixel_size)/1000); // mm to m
    set_json_object_real(id, rtn, "integrationTime",   meta.integration_time); // seconds 
    
    // Each CBF produced by the Pilatus only contains 1 image.
    set_json_object_integer(id, rtn, "first_frame", 1);
    set_json_object_integer(id, rtn, "last_frame",  1);
    set_json_object_integer(id, rtn, "frame", 1);
  }

  // Cleanup. Reminder: libcbf owns the file handle created by fopen and
  // disposes of it for us.
  if (goni) {cbf_free_goniometer(goni);}
  if (detr) {cbf_free_detector(detr);}
  cbf_free_handle(cbf);
  return rtn;
  
 error_return: // cleanup in reverse order
  if (goni) {cbf_free_goniometer(goni);}
  if (detr) {cbf_free_detector(detr);}
  if (cbf) {cbf_free_handle(cbf);}
  if (f) {fclose(f);}
  if (rtn) {json_decref(rtn);}
  return NULL;
}

/**
 * Retrieve an image from a CBF file and pass it back to
 * the caller in the same format as isRayonixGetData.
 *
 * @param[in]  fn   Filename we'd like to process
 * @param[out] imb  Image buffer we'd like to fill 
 *
 * @return 0 on success, -1 on failure
 */
int isCbfGetData(const char* fn, isImageBufType* imb) {
  static const char *id = "isCbfGetData";
  int errcode = 0;
  FILE* f = NULL;
  cbf_handle cbf = 0;
  struct cbf_dims dims;
  int bufsize = 0;

  f = fopen(fn, "r");
  if (f == NULL) {
    isLogging_err("%s: fopen failed to open CBF image file '%s'\n", id, fn);
    goto error_return;
  }

  errcode = cbf_make_handle(&cbf);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_make_handle");
    goto error_return;
  }

  errcode = cbf_read_widefile(cbf, f, MSG_NODIGEST);
  if (errcode != 0) {
    log_cbf_error(errcode, id, "cbf_read_widefile");
    goto error_return;
  }
  f = NULL; // libcbf now owns the file handle
  
  cbf_get_integerarrayparameters_wdims_fs(cbf, &(dims.compression), &(dims.binary_id), &(dims.elsize),
					  &(dims.elsigned), &(dims.elunsigned), &(dims.elements),
					  &(dims.minelement), &(dims.maxelement), &(dims.byteorder),
					  &(dims.dimfast), &(dims.dimmid), &(dims.dimslow), &(dims.padding));
  imb->buf_depth  = (int)(dims.elsize);
  imb->buf_width  = (int)(dims.dimfast);
  imb->buf_height = (int)(dims.dimmid);
  bufsize = (imb->buf_depth) * (imb->buf_width) * (imb->buf_height);
  if (bufsize <= 0) {
    isLogging_err("%s: cbf_get_integerarrayparameters_wdims_fs invalid image dimensions for file '%d', width=%u, height=%u, depth=%u\n",
		  id, fn, imb->buf_width, imb->buf_height, imb->buf_depth);
    goto error_return;
  }
  
  imb->buf = malloc(bufsize); // caller is responsible for freeing the data
  if (!imb->buf) {
    isLogging_crit("%s: Out of memory. malloc(%u) failed while processing '%s'\n",
		   id, bufsize, fn);
    goto error_return;
  }
  imb->buf_size = bufsize;

  // If "elunsigned" is true, prefer reading the image pixels
  // as unsigned values.
  // Note: This routine decompresses the data for us.
  cbf_get_image_fs(cbf, /*unused*/0, 0,
		   imb->buf, imb->buf_depth,
		   /*elsigned*/(dims.elunsigned) ? 0 : 1,
		   imb->buf_width, imb->buf_height);

  // Cleanup. Reminder: libcbf owns the file handle created by fopen and
  // disposes of it for us.
  cbf_free_handle(cbf);
  return 0;

 error_return:
  if (cbf) {cbf_free_handle(cbf);}
  if (f) {fclose(f);}
  if (imb->buf) {
    free(imb->buf);
    imb->buf = NULL;
  }
  imb->buf_size = 0;
  imb->buf_width = 0;
  imb->buf_height = 0;
  imb->buf_depth = 0;
  return -1;
}
