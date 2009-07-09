VERSION= 0.60

all: is mh

is: is.c is.h mr.c isGlobals.c imtype.c pgConn.c
	gcc -Wall -O3 -o is is.c isGlobals.c mr.c imtype.c pgConn.c  -ltiff -ljpeg -lm -lpq

mh: mh.c
	gcc -Wall -o mh mh.c

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

