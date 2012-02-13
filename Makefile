Version=2.11

all: copyright is isp isdebug Makefile

copyright:
	@echo " "
	@echo "Copyright (C) 2009-2011 by Keith Brister"
	@echo "All rights reserved."
	@echo " "

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
	install isScreenrc /pf/etc
	install ls_run_is /pf/bin
	install is /pf/bin/linux-x86_64
	install -d /usr/lib/ocf/resource.d/ls-cat
	install -t /usr/lib/ocf/resource.d/ls-cat  ImageServer.py 
