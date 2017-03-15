all: is isUtilities.o isBlank.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o

isData.o: isData.c is.h Makefile
	gcc -g -Wall -c isData.c

isWorker.o: isWorker.c is.h Makefile
	gcc -g -Wall -c isWorker.c

isProcessManagement.o: isProcessManagement.c is.h Makefile
	gcc -g -Wall -c isProcessManagement.c

isUtilities.o: isUtilities.c is.h Makefile
	gcc -g -Wall -c isUtilities.c 

isBlank.o: isBlank.c is.h Makefile
	gcc -g -Wall -c isBlank.c

isH5.o: isH5.c is.h Makefile
	gcc -g -Wall -c isH5.c

isRayonix.o: isRayonix.c is.h Makefile
	gcc -g -Wall -c isRayonix.c

is: isMain.c is.h Makefile isUtilities.o isBlank.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o
	gcc -g -Wall isMain.c -o is isUtilities.o isBlank.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o -lhiredis -ljansson -lhdf5 -ltiff -lcrypto -lm -pthread

