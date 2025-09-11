/** @file isRayonix.c
 ** @copyright 2017 by Northwestern University All Rights Reservered
 ** @author Keith Brister
 ** @brief Code to read Rayonix TIFF image files for the LS-CAT Image Server Version 2
 */
#include "is.h"
/*
**
** The frame structure and definitions may be covered by copyrights
** owned by Rayonix LLC.  Used at LS-CAT by permission of MarUSA Inc.,
** now Rayonix LLC.
**
** Modifications for support by the LS-CAT image server:
** Copyright (C) 2009-2010, 2017 by Northwestern University
** Author: Keith Brister
**
** All rights reserved.
**
*/

/** uint16_t                            */
#define UINT16  unsigned short

/** int16_t                             */
#define INT16 short

/** uint32_t                            */
#define UINT32  unsigned int

/** int32_t                             */
#define INT32 int
/*
   Currently frames are always written as defined below:
   origin=UPPER_LEFT
   orientation=HFAST
   view_direction=FROM_SOURCE
*/

/* This number is  written into the byte_order fields in the
   native byte order of the machine writing the file */
//#define LITTLE_ENDIAN 1234
//#define BIG_ENDIAN  4321

/* possible orientations of frame data (stored in orienation field)     */
#define HFAST     0  /*!< Horizontal axis is fast                       */
#define VFAST     1  /*!< Vertical axis is fast                         */

/* possible origins of frame data (stored in origin field)              */
#define UPPER_LEFT    0         /*!< origin is upper left               */
#define LOWER_LEFT    1         /*!< origin is lower left               */
#define UPPER_RIGHT   2         /*!< origin is upper right              */
#define LOWER_RIGHT   3         /*!< origin is lower right              */

/* possible view directions of frame data for
   the given orientation and origin (stored in view_direction field)    */
#define FROM_SOURCE   0         /*!< We are riding the x-ray beam       */
#define TOWARD_SOURCE 1         /*!< We are looing in to the beam       */

#define MAXIMAGES 9             /*!< Some array sizes, not sure why.    */

/** Header structure for Rayonix Tiff images.
 */
typedef struct frame_header_type {
  /* File/header format parameters (256 bytes) */
  UINT32        header_type;            //!< flag for header type  (can be used as magic number)
  char header_name[16];                 //!< header name (MARCCD)
  UINT32        header_major_version;   //!< header_major_version (n.)
  UINT32        header_minor_version;   //!< header_minor_version (.n)
  UINT32        header_byte_order;      //!< BIG_ENDIAN (Motorola,MIPS); LITTLE_ENDIAN (DEC, Intel)
  UINT32        data_byte_order;        //!< BIG_ENDIAN (Motorola,MIPS); LITTLE_ENDIAN (DEC, Intel)
  UINT32        header_size;            //!< in bytes
  UINT32        frame_type;             //!< flag for frame type
  UINT32        magic_number;           //!< to be used as a flag - usually to indicate new file
  UINT32        compression_type;       //!< type of image compression
  UINT32        compression1;           //!< compression parameter 1
  UINT32        compression2;           //!< compression parameter 2
  UINT32        compression3;           //!< compression parameter 3
  UINT32        compression4;           //!< compression parameter 4
  UINT32        compression5;           //!< compression parameter 4
  UINT32        compression6;           //!< compression parameter 4
  UINT32        nheaders;               //!< total number of headers
  UINT32        nfast;                  //!< number of pixels in one line
  UINT32        nslow;                  //!< number of lines in image
  UINT32        depth;                  //!< number of bytes per pixel
  UINT32        record_length;          //!< number of pixels between succesive rows
  UINT32        signif_bits;            //!< true depth of data, in bits
  UINT32        data_type;              //!< (signed,unsigned,float...)
  UINT32        saturated_value;        //!< value marks pixel as saturated
  UINT32        sequence;               //!< TRUE or FALSE
  UINT32        nimages;                //!< total number of images - size of each is nfast*(nslow/nimages)
  UINT32        origin;                 //!< corner of origin
  UINT32        orientation;            //!< direction of fast axis
  UINT32        view_direction;         //!< direction to view frame
  UINT32        overflow_location;      //!< FOLLOWING_HEADER, FOLLOWING_DATA
  UINT32        over_8_bits;            //!< # of pixels with counts > 255
  UINT32        over_16_bits;           //!< # of pixels with count > 65535
  UINT32        multiplexed;            //!< multiplex flag
  UINT32        nfastimages;            //!< # of images in fast direction
  UINT32        nslowimages;            //!< # of images in slow direction
  UINT32        darkcurrent_applied;    //!< flags correction has been applied - hold magic number ?
  UINT32        bias_applied;           //!< flags correction has been applied - hold magic number ?
  UINT32        flatfield_applied;      //!< flags correction has been applied - hold magic number ?
  UINT32        distortion_applied;     //!< flags correction has been applied - hold magic number ?
  UINT32        original_header_type;   //!< Header/frame type from file that frame is read from
  UINT32        file_saved;             //!< Flag that file has been saved, should be zeroed if modified
  UINT32        n_valid_pixels;         //!< Number of pixels holding valid data - first N pixels
  UINT32        defectmap_applied;      //!< flags correction has been applied - hold magic number ?
  UINT32        subimage_nfast;         //!< when divided into subimages (eg. frameshifted)
  UINT32        subimage_nslow;         //!< when divided into subimages (eg. frameshifted)
  UINT32        subimage_origin_fast;   //!< when divided into subimages (eg. frameshifted)
  UINT32        subimage_origin_slow;   //!< when divided into subimages (eg. frameshifted)
  UINT32        readout_pattern;        //!< BIT Code - 1 = A, 2 = B, 4 = C, 8 = D
  UINT32        saturation_level;       //!< at this value and above, data are not reliable
  UINT32        orientation_code;       //!< Describes how this frame needs to be rotated to make it "right"
  UINT32        frameshift_multiplexed; //!< frameshift multiplex flag.
  char reserve1[(64-50)*sizeof(INT32)-16];      //!< padding

  /** Data statistics (128) */
  UINT32        total_counts[2];          //!< 64 bit integer range = 1.85E19
  UINT32        special_counts1[2];       //!< unknown use
  UINT32        special_counts2[2];       //!< unknown use
  UINT32        min;                      //!< minium pixel value
  UINT32        max;                      //!< maximum pixel value
  UINT32        mean;                     //!< mean * 1000 */
  UINT32        rms;                      //!< rms * 1000 */
  UINT32        n_zeros;                  //!< number of pixels with 0 value  - not included in stats in unsigned data */
  UINT32        n_saturated;              //!< number of pixels with saturated value - not included in stats */
  UINT32        stats_uptodate;           //!< Flag that stats OK - ie data not changed since last calculation */
  UINT32        pixel_noise[MAXIMAGES];   //!< 1000*base noise value (ADUs) */
  char reserve2[(32-13-MAXIMAGES)*sizeof(INT32)];       //!< padding

#if 0
  /* More statistics (256) */
  UINT16 percentile[128];
#else
  /* Sample Changer info */
  char          barcode[16];            //!< not used at LS-CAT
  UINT32        barcode_angle;          //!< not used at LS-CAT
  UINT32        barcode_status;         //!< not used at LS-CAT
  /* Pad to 256 bytes */
  char reserve2a[(64-6)*sizeof(INT32)]; //!< pad
#endif

  /* Goniostat parameters (128 bytes) */
  INT32 xtal_to_detector;       //!< 1000*distance in millimeters
  INT32 beam_x;                 //!< 1000*x beam position (pixels)
  INT32 beam_y;                 //!< 1000*y beam position (pixels)
  INT32 integration_time;       //!< integration time in milliseconds
  INT32 exposure_time;          //!< exposure time in milliseconds
  INT32 readout_time;           //!< readout time in milliseconds
  INT32 nreads;                 //!< number of readouts to get this image
  INT32 start_twotheta;         //!< 1000*two_theta angle
  INT32 start_omega;            //!< 1000*omega angle
  INT32 start_chi;              //!< 1000*chi angle
  INT32 start_kappa;            //!< 1000*kappa angle
  INT32 start_phi;              //!< 1000*phi angle
  INT32 start_delta;            //!< 1000*delta angle
  INT32 start_gamma;            //!< 1000*gamma angle
  INT32 start_xtal_to_detector; //!< 1000*distance in mm (dist in um)
  INT32 end_twotheta;           //!< 1000*two_theta angle
  INT32 end_omega;              //!< 1000*omega angle
  INT32 end_chi;                //!< 1000*chi angle
  INT32 end_kappa;              //!< 1000*kappa angle
  INT32 end_phi;                //!< 1000*phi angle
  INT32 end_delta;              //!< 1000*delta angle
  INT32 end_gamma;              //!< 1000*gamma angle
  INT32 end_xtal_to_detector;   //!< 1000*distance in mm (dist in um)
  INT32 rotation_axis;          //!< active rotation axis (index into above ie. 0=twotheta,1=omega...)
  INT32 rotation_range;         //!< 1000*rotation angle
  INT32 detector_rotx;          //!< 1000*rotation of detector around X
  INT32 detector_roty;          //!< 1000*rotation of detector around Y
  INT32 detector_rotz;          //!< 1000*rotation of detector around Z
  INT32 total_dose;             //!< Hz-sec (counts) integrated over full exposure
  char reserve3[(32-29)*sizeof(INT32)]; //!< Pad Gonisotat parameters to 128 bytes

  /* Detector parameters (128 bytes) */
  INT32 detector_type;                          //!< detector type
  INT32 pixelsize_x;                            //!< pixel size (nanometers)
  INT32 pixelsize_y;                            //!< pixel size (nanometers)
  INT32 mean_bias;                              //!< 1000*mean bias value
  INT32 photons_per_100adu;                     //!< photons / 100 ADUs
  INT32 measured_bias[MAXIMAGES];               //!< 1000*mean bias value for each image
  INT32 measured_temperature[MAXIMAGES];        //!< Temperature of each detector in milliKelvins
  INT32 measured_pressure[MAXIMAGES];           //!< Pressure of each chamber in microTorr

  /* Retired reserve4 when MAXIMAGES set to 9 from 16 and two fields removed, and temp and pressure added
  char reserve4[(32-(5+3*MAXIMAGES))*sizeof(INT32)];
  */

  /* X-ray source and optics parameters (128 bytes) */
  /* X-ray source parameters (14*4 bytes) */
  INT32 source_type;            //!< (code) - target, synch. etc
  INT32 source_dx;              //!< Optics param. - (size microns)
  INT32 source_dy;              //!< Optics param. - (size microns)
  INT32 source_wavelength;      //!< wavelength (femtoMeters)
  INT32 source_power;           //!< (Watts)
  INT32 source_voltage;         //!< (Volts)
  INT32 source_current;         //!< (microAmps)
  INT32 source_bias;            //!< (Volts)
  INT32 source_polarization_x;  //!< ()
  INT32 source_polarization_y;  //!< ()
  INT32 source_intensity_0;     //!< (arbitrary units)
  INT32 source_intensity_1;     //!< (arbitrary units)
  char reserve_source[2*sizeof(INT32)]; //!< padding

  /* X-ray optics_parameters (8*4 bytes) */
  INT32 optics_type;            //!< Optics type (code)
  INT32 optics_dx;              //!< Optics param. - (size microns)
  INT32 optics_dy;              //!< Optics param. - (size microns)
  INT32 optics_wavelength;      //!< Optics param. - (size microns)
  INT32 optics_dispersion;      //!< Optics param. - (*10E6)
  INT32 optics_crossfire_x;     //!< Optics param. - (microRadians)
  INT32 optics_crossfire_y;     //!< Optics param. - (microRadians)
  INT32 optics_angle;           //!< Optics param. - (monoch. 2theta - microradians)
  INT32 optics_polarization_x;  //!< ()
  INT32 optics_polarization_y;  //!< ()
  char reserve_optics[4*sizeof(INT32)]; //!< padding

  char reserve5[((32-28)*sizeof(INT32))];       //!< Pad X-ray parameters to 128 bytes

  /* File parameters (1024 bytes) */
  char filetitle[128];                          //!< Title
  char filepath[128];                           //!< path name for data file
  char filename[64];                            //!< name of data file
  char acquire_timestamp[32];                   //!< date and time of acquisition
  char header_timestamp[32];                    //!< date and time of header update
  char save_timestamp[32];                      //!< date and time file saved
  char file_comment[512];                       //!< comments  - can be used as desired
  char reserve6[1024-(128+128+64+(3*32)+512)];  //!< Pad File parameters to 1024 bytes

  /* Dataset parameters (512 bytes) */
  char dataset_comment[512];  //!< comments  - can be used as desired

  /* Reserved for user definable data - will not be used by Mar! */
  char user_data[512];                          //!< User defined data (unused)

  /* char pad[----] USED UP! */     /* pad out to 3072 bytes */

} frame_header;


/** Some important items have no place in the Rayonix header so we place the in the comments.
 ** The comment string might look something like this:
 ** detector='Rayonix MX-225 s/n 001' LS_CAT_Beamline='21-ID-E' kappa=0.0 omega=235.0
 **
 ** This routine attempts to parse out the detector or LS_CAT_Beamline parameters and return them in a malloc char array
 **
 ** @param cp           Pointer to the comment string.
 **
 ** @param needle       parameter to search for
 **
 ** @returns string with our parameter's value or null if not found.  Be sure to free it later.
 **
 */
char *parseComment( char const *cp, char const *needle) {
  static const char *id = "parseComment";
  char *p;    // pointer into the comment string
  char *rtn;  // our return value
  int i;  // index into comment
  int j;  // index into detector info

  p = strstr( cp, needle);
  if( p == NULL)
    //
    // Not found
    //
    return NULL;


  rtn = malloc( strlen( cp));   // a little too much memory is allocated, better that way then the other way around
  if( rtn == NULL) {
    isLogging_crit("%s: Out of memory\n", id);
    return NULL;
  }

  //
  // Simple parser, assume that needle includes the opening single quote and we stop and the next one we see
  // Probably we should make the fancier.  TODO: make this work with more complicated input.
  //

  for( i=strlen(needle), j=0; p[i] && p[i] != '\''; i++, j++) {
    rtn[j] = p[i];
  }
  rtn[j] = 0;   // terminate the string;

  return rtn;
}

/** Retrieve only the meta data from the image file.
 **
 ** @param fn Our file name
 **
 ** @returns JSON object chock full of  our metadata.
 */
json_t *isRayonixGetMeta( isWorkerContext_t *wctx, const char *fn) {
  static const char *id = "marTiffGetHeader";
  //
  // is is the image structure we are getting all our info from
  // pad is the amount of extra room to leave on the RHS
  // in addtion to extra scans line at the top and bottom
  //
  FILE *f;
  frame_header fh;
  char *tmps;
  json_t *rtn;

  rtn = json_object();
  if (rtn == NULL) {
    isLogging_err("%s: Could not create return JSON object\n", id);
    return NULL;
  }

  // Get the header
  //
  f = fopen( fn, "r");
  if( f == NULL) {
    isLogging_err("%s: fopen failed to open MarCCD image file '%s'\n", id, fn);
    json_decref(rtn);
    return NULL;
  }

  fseek( f, 1024, SEEK_SET);

  fread( &fh, sizeof(frame_header), 1, f);
  fclose( f);

  pthread_mutex_lock(&wctx->metaMutex);
  set_json_object_string(id, rtn, "filename", fh.filename);
  set_json_object_string(id, rtn, "filepath", fh.filepath);
  set_json_object_string(id, rtn, "comment",  fh.file_comment);

  tmps = parseComment( fh.file_comment, "detector='");
  //
  // Old images (pre 3/9/2011) do not have the detector parameter, guess
  //
  if( tmps == NULL) {
    if( fh.nslow == 3072 || fh.nslow == 1536 || fh.nslow == 768)
      tmps = strdup( "Rayonix MX-225");
    else if( fh.nslow == 4096 || fh.nslow == 2048 || fh.nslow == 1024)
      tmps = strdup( "Rayonix MX-300");
    else
      tmps = strdup( "Unknown");
  }
  set_json_object_string(id, rtn, "detector", tmps);
  free(tmps);

  tmps  = parseComment( fh.file_comment, "LS_CAT_Beamline='");
  if( tmps == NULL) {
    tmps = strdup( "21-ID");
  }
  set_json_object_string(id, rtn, "beamline", tmps);

  set_json_object_real(id, rtn, "detector_distance", fh.xtal_to_detector/1000000.0); // um to m
  set_json_object_real(id, rtn, "rotationRange",     fh.rotation_range/1000.0);
  set_json_object_real(id, rtn, "startPhi",          fh.start_phi/1000.0);
  set_json_object_real(id, rtn, "photon_energy",     12398.4193 / fh.source_wavelength * 100000.0);
  set_json_object_real(id, rtn, "wavelength",        fh.source_wavelength/100000.0); // femtometers to angstroms
  set_json_object_real(id, rtn, "beam_center_x",     fh.beam_x/1000.0);
  set_json_object_real(id, rtn, "beam_center_y",     fh.beam_y/1000.0);
  set_json_object_real(id, rtn, "x_pixel_size",      fh.pixelsize_x/1e9);
  set_json_object_real(id, rtn, "y_pixel_size",      fh.pixelsize_y/1e9);
  set_json_object_real(id, rtn, "integrationTime",   fh.integration_time/1000.0);
  set_json_object_real(id, rtn, "exposureTime",      fh.exposure_time/1000.0);
  set_json_object_real(id, rtn, "readoutTime",       fh.readout_time/1000.0);
  set_json_object_real(id, rtn, "meanValue",         fh.mean/1000.0);
  set_json_object_real(id, rtn, "rmsValue",          fh.rms/1000.0);

  set_json_object_integer(id, rtn, "nSaturated",   fh.n_saturated);
  set_json_object_integer(id, rtn, "x_pixels_in_detector", fh.nfast);
  set_json_object_integer(id, rtn, "y_pixels_in_detector", fh.nslow);
  set_json_object_integer(id, rtn, "image_depth",  fh.depth);
  set_json_object_integer(id, rtn, "saturation",   fh.saturation_level);
  set_json_object_integer(id, rtn, "minValue",     fh.min);
  set_json_object_integer(id, rtn, "maxValue",     fh.max);

  //  is->b->h_dist                  = fh.xtal_to_detector/1000.0;
  //  is->b->h_rotationRange         = fh.rotation_range/1000.0;
  //  is->b->h_startPhi              = fh.start_phi/1000.0;
  //  is->b->h_wavelength            = fh.source_wavelength/100000.0;
  //  is->b->h_beamX                 = fh.beam_x/1000.0;
  //  is->b->h_beamY                 = fh.beam_y/1000.0;
  //  is->b->h_imagesizeX            = fh.nfast;
  //  is->b->h_imagesizeY            = fh.nslow;
  //  is->b->h_pixelsizeX            = fh.pixelsize_x/1000.0;
  //  is->b->h_pixelsizeY            = fh.pixelsize_y/1000.0;
  //  is->b->h_integrationTime       = fh.integration_time/1000.0;
  //  is->b->h_exposureTime          = fh.exposure_time/1000.0;
  //  is->b->h_readoutTime           = fh.readout_time/1000.0;
  //  is->b->h_saturation            = fh.saturation_level;
  //  is->b->h_minValue              = fh.min;
  //  is->b->h_maxValue              = fh.max;
  //  is->b->h_meanValue             = fh.mean/1000.0;
  //  is->b->h_rmsValue              = fh.rms/1000.0;
  //  is->b->h_nSaturated            = fh.n_saturated;

  // Each MarCCD file contains only 1 image.
  set_json_object_integer(id, rtn, "first_frame", 1);
  set_json_object_integer(id, rtn, "last_frame",  1);
  set_json_object_string(id, rtn, "fn", fn);
  set_json_object_integer(id, rtn, "frame", 1);
  
  pthread_mutex_unlock(&wctx->metaMutex);

  return rtn;
}

void is_tiff_error_handler(const char *module, const char *fmt, va_list arg_ptr) {
  //  va_list arg_ptr;

  if (module) {
    syslog(LOG_ERR, "%s", module);
  }
  vsyslog(LOG_ERR, fmt, arg_ptr);
}

void is_tiff_warning_handler(const char *module, const char *fmt, va_list arg_ptr) {
  //  va_list arg_ptr;

  if (module) {
    syslog(LOG_WARNING, "%s", module);
  }
  vsyslog(LOG_WARNING, fmt, arg_ptr);
}

/** Retreive a buffer full of image
 **
 ** @param[in]  fn   Filename we'd like to process
 **
 ** @param[out] imb  Image buffer we'd like to fill 
 **
 ** @returns 0 on success, -1 on failure
 */
int isRayonixGetData( isWorkerContext_t *wctx, const char *fn, isImageBufType **imbp) {
  static const char *id = "marTiffGetData";

  //
  // is is the image structure we are getting all our info from
  // pad is the amount of extra room to leave on the RHS
  // in addtion to extra scans line at the top and bottom
  //
  TIFF *tf;
  int i;
  struct sigaction signew;
  struct sigaction sigold;
  jmp_buf jmpenv;
  unsigned int inHeight;
  unsigned int inWidth;
  unsigned short *buf;
  int buf_size;
  isImageBufType *imb;

  void sigbusHandler( int sig) {
    longjmp( jmpenv, 1);
  }

  imb = *imbp;

  if( setjmp( jmpenv)) {
    isLogging_err("%s: Caught bus error, trying to recover\n", id);
    sleep( 1);
  }

  signew.sa_handler = sigbusHandler;
  signew.sa_sigaction = NULL;
  if(  sigaction( SIGBUS, &signew, &sigold) != 0) {
    isLogging_err("%s: Error setting sigaction\n", id);
  }

  TIFFSetErrorHandler(is_tiff_error_handler);
  TIFFSetWarningHandler(NULL);   // surpress annoying warning messages 
  tf = TIFFOpen( fn, "r");       // open the file
  if( tf == NULL) {
    isLogging_err("%s: marTiffRead failed to open file '%s'\n", id, fn);
    signew.sa_handler = SIG_DFL;
    sigaction( SIGBUS, &signew, NULL);
    return -1;
  }
  TIFFGetField( tf, TIFFTAG_IMAGELENGTH,   &inHeight);
  TIFFGetField( tf, TIFFTAG_IMAGEWIDTH,    &inWidth);

  //
  buf_size = inWidth * inHeight * sizeof( unsigned short);
  buf  = malloc( buf_size);
  if( buf == NULL) {
    TIFFClose( tf);
    isLogging_crit("%s: Out of memory.  malloc(%d) failed while processing '%s'\n",
		   id, buf_size, fn);
    signew.sa_handler = SIG_DFL;
    sigaction( SIGBUS, &signew, NULL);
    exit (-1);
  }
  //
  // read the image
  //
  for( i=0; i<inHeight; i++) {
    TIFFReadScanline( tf, buf + i*(inWidth), i, 0);
  }
  //
  // we are done
  //
  TIFFClose( tf);
  signew.sa_handler = SIG_DFL;
  sigaction( SIGBUS, &signew, NULL);
  //
  //
  imb->buf_size   = buf_size;
  imb->buf_width  = inWidth;
  imb->buf_height = inHeight;
  imb->buf_depth  = 2;
  imb->buf = buf;
  return 0;
}
