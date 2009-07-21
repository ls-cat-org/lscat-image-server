VERSION= 2.00

all: is isp mh isdebug

is: is.c is.h mr.c isGlobals.c imtype.c pgConn.c ib.c Makefile
	gcc -Wall -O3 -ffast-math -o is is.c isGlobals.c ib.c mr.c imtype.c pgConn.c  -ltiff -ljpeg -lm -lpq

isp: is.c is.h mr.c isGlobals.c imtype.c pgConn.c ib.c Makefile
	gcc -Wall -g -O3 -ffast-math -DPROFILE -o isp is.c isGlobals.c ib.c mr.c imtype.c pgConn.c  -ltiff -ljpeg -lm -lpq -pg

isdebug: is.c is.h mr.c isGlobals.c imtype.c pgConn.c ib.c Makefile
	gcc -Wall -g -ffast-math -o isdebug is.c isGlobals.c ib.c mr.c imtype.c pgConn.c  -ltiff -ljpeg -lm -lpq

clean:
	rm -f is *.jpeg *.o *~

dist:
	ln -fs . is-$(VERSION)
	tar czvf is-$(VERSION).tar.gz is-$(VERSION)/*.c is-$(VERSION)/*.h is-$(VERSION)/Makefile is-$(VERSION)/*Wrap is-$(VERSION)/etcService is-$(VERSION)/carpsIS is-$(VERSION)/mhxml
	cd is-$(VERSION)
	rm -f is-$(VERSION)

install: is
	install mh /usr/local/bin
	install is /usr/local/bin

