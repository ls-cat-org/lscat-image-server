all: is

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
	install --mode=755 is /usr/local/bin
	install --mode=644 is.conf /etc/rsyslog.d
	install --mode=644 is-dev.conf /etc/rsyslog.d
	systemctl restart rsyslog.service
	install --mode=644 is.logrotate /etc/logrotate.d/is

isLogging.o: isLogging.c is.h Makefile
	gcc -g -Wall -c isLogging.c

isData.o: isData.c is.h Makefile
	gcc -g -Wall -c isData.c

isWorker.o: isWorker.c is.h Makefile
	gcc -g -Wall -c isWorker.c

isProcessManagement.o: isProcessManagement.c is.h Makefile
	gcc -g -Wall -c isProcessManagement.c

isUtilities.o: isUtilities.c is.h Makefile
	gcc -g -Wall -c isUtilities.c 

isH5.o: isH5.c is.h Makefile
	gcc -g -Wall -c isH5.c

isRayonix.o: isRayonix.c is.h Makefile
	gcc -g -Wall -c isRayonix.c

isReduceImage.o: isReduceImage.c is.h Makefile
	gcc -g -Wall -c isReduceImage.c

isJpeg.o: isJpeg.c is.h Makefile
	gcc -g -Wall -c isJpeg.c

isIndex.o: isIndex.c is.h Makefile
	gcc -g -Wall -c isIndex.c

isSpots.o: isSpots.c is.h Makefile
	gcc -g -Wall -c isSpots.c

isBitmapFont.o: isBitmapFont.c is.h Makefile
	gcc -g -Wall -c isBitmapFont.c

is: isMain.c is.h Makefile isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o isReduceImage.o isJpeg.o isBitmapFont.o isIndex.o isSpots.o isLogging.o
	gcc -g -Wall isMain.c -L /usr/local/lib64 -L /usr/local/lib -L/usr/lib -o is isLogging.o isUtilities.o isH5.o isRayonix.o isProcessManagement.o isWorker.o isData.o isReduceImage.o isJpeg.o isIndex.o isSpots.o isBitmapFont.o -lhiredis -ljansson -lhdf5 -ltiff -lcrypto -ljpeg -lm -lzmq -pthread
