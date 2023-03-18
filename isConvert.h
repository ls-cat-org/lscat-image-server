#ifndef IS_CONVERT_H
#define IS_CONVERT_H

#include <jansson.h>
#include <hdf5.h>

#include "is.h"

/** h5 to json equivalencies.  We read HDF5 properties and convert
 ** them to json to use and/or transmit back to the user's browser.
 */
struct h5_json_property {
  const char *h5_location;  //!< HDF5 property name
  const char *json_name;    //!< JSON equivalent
};

// Get the software_version first so we can determine which params we have.
extern const struct h5_json_property json_convert_software_version;

/**
 * Our mapping between hdf5 file properties and our metadata object properties for 
 * datasets produced by the Eiger 2X 16M, and all other v1.8 DCUs.
 */
extern const struct h5_json_property json_convert_array_1_8[];

/**
 * Our mapping between hdf5 file properties and our metadata object properties for 
 * datasets produced by the Eiger 9M, and all other v1.6 DCUs.
 */
extern const struct h5_json_property json_convert_array_1_6[];

/**
 * Converts an HDF5 Dataset (a single property within a .h5 file) into a JSON object.
 * @param [in] file An HDF5 file handle created by H5Fopen.
 * @param [in] property A mapping of an HDF5 dataset to a desired output JSON field name.
 * @return A jansson library-internal representation of a JSON object. Returns NULL on failure.
 */
extern json_t* h5_property_to_json(hid_t file, const struct h5_json_property* property);

/**
 * Gets the Dectris DCU software version which produced the specified HDF5 archive (i.e. master file).
 *
 * @param [in] file An HDF5 archive (.h5 file), i.e. the "master file" containing image metadata.
 * @return The software version of a Dectris detector DCU. Returns NULL on failure.
 *
 * @warning  { This function creates a string using malloc(), but caller is responsible for calling
 *             free() when no longer needed. }
 */
extern json_t* get_dcu_version(hid_t file);
#endif // Header guard
