
CC=gcc
CFLAGS=-std=gnu99 -g -Wall -D_GNU_SOURCE -I /usr/include/hdf5/serial -L /usr/lib/x86_64-linux-gnu/hdf5/serial -L /usr/local/lib64 -L /usr/local/lib -L/usr/lib

all: is isConvertTest

distclean:
	@rm -f *.o is
	@rm -rf docs

clean:
	@rm -f *.o is

.PHONY: docs
docs:
	doxygen isDoxygen.config
	(cd docs/latex; make)

install:
	install --mode=0644 lscat-image-server.service /lib/systemd/system
	install --mode=0755 kill-is /usr/local/bin
	install --mode=0755 is /usr/local/bin
	install --mode=0644 ls-ee-contrabass-pubkey.pem /etc
	install --mode=0644 is.conf /etc/rsyslog.d
	install --mode=0644 is-dev.conf /etc/rsyslog.d
	systemctl restart rsyslog.service
	install --mode=0644 is.logrotate /etc/logrotate.d/is
	@echo "catsok is installed, please run 'systemctl enable --now lscat-image-server.service' when you are ready to run it."

isLogging.o: isLogging.c is.h Makefile
	$(CC) $(CFLAGS) -c isLogging.c

isConvert.o: isConvert.c is.h Makefile
	$(CC) $(CFLAGS) -c isConvert.c

isData.o: isData.c is.h Makefile
	$(CC) $(CFLAGS) -c isData.c

isWorker.o: isWorker.c is.h Makefile
	$(CC) $(CFLAGS) -c isWorker.c

isProcessManagement.o: isProcessManagement.c is.h Makefile
	$(CC) $(CFLAGS) -c isProcessManagement.c

isUtilities.o: isUtilities.c is.h Makefile
	$(CC) $(CFLAGS) -c isUtilities.c 

isH5.o: isH5.c is.h Makefile
	$(CC) $(CFLAGS) -c isH5.c

isRayonix.o: isRayonix.c is.h Makefile
	$(CC) $(CFLAGS) -c isRayonix.c

isReduceImage.o: isReduceImage.c is.h Makefile
	$(CC) $(CFLAGS) -c isReduceImage.c

isJpeg.o: isJpeg.c is.h Makefile
	$(CC) $(CFLAGS) -c isJpeg.c

isIndex.o: isIndex.c is.h Makefile
	$(CC) $(CFLAGS) -c isIndex.c

isSpots.o: isSpots.c is.h Makefile
	$(CC) $(CFLAGS) -c isSpots.c

isBitmapFont.o: isBitmapFont.c is.h Makefile
	$(CC) $(CFLAGS) -c isBitmapFont.c

isSubProcess.o: isSubProcess.c is.h Makefile
	$(CC) $(CFLAGS) -c isSubProcess.c

isConvertTest: isConvertTest.c is.h Makefile isConvert.o isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o isReduceImage.o isJpeg.o isBitmapFont.o isIndex.o isSpots.o isLogging.o isSubProcess.o
	$(CC) $(CFLAGS) isConvertTest.c -o isConvertTest isLogging.o isConvert.o isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isSubProcess.o isData.o isReduceImage.o isJpeg.o isIndex.o isSpots.o isBitmapFont.o -lhiredis -ljansson -lhdf5 -ltiff -lcrypto -ljpeg -lm -lzmq -pthread

is: isMain.c is.h Makefile isConvert.o isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o isReduceImage.o isJpeg.o isBitmapFont.o isIndex.o isSpots.o isLogging.o isSubProcess.o
	$(CC) $(CFLAGS) isMain.c -o is isLogging.o isConvert.o isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isSubProcess.o isData.o isReduceImage.o isJpeg.o isIndex.o isSpots.o isBitmapFont.o -lhiredis -ljansson -lhdf5 -ltiff -lcrypto -ljpeg -lm -lzmq -pthread
