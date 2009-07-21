#include "is.h"

int debug = 0;

sem_t workerSem;
pthread_mutex_t ibUseMutex;
pthread_rwlock_t isTypeChangeLock;
pthread_mutex_t workerMutex;
pthread_cond_t workerCond;

isType isQueue;
int    isQueueLength = 0;
pthread_t kbt[NTHREADS];
imBufType ibs[NIBUFS];

//imtype_type mar345 = {
//  0, 0, 1, 0x00, 0x0000, 0x000004d2, "Mar 345", mar345
//};

//imtype_type mar345BS = {
//  0, 0, 1, 0x00, 0x0000, 0xd2040000, "Mar 345 Byte Swapped", mar345
//};

imtype_type martiff = {
  0, 0, 1, 0x00, 0x0000, 0x002a4949, "Mar 165", marTiffGetHeader, marTiffGetData
};

imtype_type martiffBS = {
  0, 0, 1, 0x00, 0x0000, 0x49492a00, "Mar 165 Byte Swapped", marTiffGetHeader, marTiffGetData
};

//imtype_type adsc = {
//  1, 0, 0, '{', 0x0000, 0x00000000, "ADSC", adsc
//};


imtype_type *imTypeArray[] = {
  &martiff, &martiffBS, NULL
};


/*

  mar345 first word is "1234" or
  00 00 04 D2
  Byte-swapped is
  D2 04 00 00
  
  adsc first word is 88607355 or
  45 48 0a 7b
  or {\nHE


  martiff
  2a4949

*/



//adsc_header_type *theHeader = NULL;

//mar345_ascii_header_type *theMar345AsciiHeader = NULL;

//mar345_bin_header_type theMar345BinHeader;
