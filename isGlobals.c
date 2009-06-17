#include "is.h"

int debug = 0;

imtype_type mar345 = {
  0, 0, 1, 0x00, 0x0000, 0x000004d2, "Mar 345", mar3452jpeg
};

imtype_type mar345BS = {
  0, 0, 1, 0x00, 0x0000, 0xd2040000, "Mar 345 Byte Swapped", mar3452jpeg
};

imtype_type mar165 = {
  0, 0, 1, 0x00, 0x0000, 0x002a4949, "Mar 165", marTiff2png
};

imtype_type mar165BS = {
  0, 0, 1, 0x00, 0x0000, 0x49492a00, "Mar 165 Byte Swapped", marTiff2png
};

imtype_type adsc = {
  1, 0, 0, '{', 0x0000, 0x00000000, "ADSC", adsc2jpeg
};


imtype_type *imTypeArray[] = {
  &mar345, &mar345BS, &mar165, &mar165BS, &adsc, NULL
};


/*

  mar345 first word is "1234" or
  00 00 04 D2
  Byte-swapped is
  D2 04 00 00
  
  adsc first word is 88607355 or
  45 48 0a 7b
  or {\nHE


  mar165
  2a4949

*/



adsc_header_type *theHeader = NULL;

mar345_ascii_header_type *theMar345AsciiHeader = NULL;

mar345_bin_header_type theMar345BinHeader;
