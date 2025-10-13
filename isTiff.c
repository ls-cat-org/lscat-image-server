#include <tiffio.h>
#include "is.h"

static void tiffio_error_handler(const char *module, const char *fmt, va_list arg_ptr) {
  if (module) {
    syslog(LOG_ERR, "%s", module);
  }
  vsyslog(LOG_ERR, fmt, arg_ptr);
}

json_t *isTiffGetMeta(const char *fn) {
  static const char *id = "isTiffGetMeta";
  json_t* rtn = NULL;
  TIFF* tf    = NULL;
  unsigned int imgHeight = 0;
  unsigned int imgWidth  = 0;
  
  rtn = json_object();
  if (rtn == NULL) {
    isLogging_err("%s: Could not create return JSON object\n", id);
    goto error_return;
  }

  TIFFSetErrorHandler(tiffio_error_handler);
  TIFFSetWarningHandler(NULL); // surpress annoying warning messages 
  tf = TIFFOpen(fn, "r");
  if (tf == NULL) {
    isLogging_err("%s: failed to open TIFF file '%s'\n", id, fn);
    goto error_return;
  }
  TIFFGetField(tf, TIFFTAG_IMAGEWIDTH,  &imgWidth);
  TIFFGetField(tf, TIFFTAG_IMAGELENGTH, &imgHeight);
  
  set_json_object_integer(id, rtn, "x_pixels_in_detector", (int)imgWidth);
  set_json_object_integer(id, rtn, "y_pixels_in_detector", (int)imgHeight);

  TIFFClose(tf);
  return rtn;

 error_return:
  if (tf)  {TIFFClose(tf);}
  if (rtn) {json_decref(rtn);}
  return NULL;
}


int isTiffGetData(const char *fn, isImageBufType* imb) {
  return isRayonixGetData(fn, imb);
}
