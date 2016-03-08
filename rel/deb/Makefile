VERSION = $(BUILD_NUMBER) # THis does not seem to work at the moment
VERSION ?= 0
COMPONENT = ddb

.PHONY: prepare package

package: clean prepare
#	We keep actuall packaging in jenkins for now
#	dpkg-deb -b stage ddb_$(VERSION)

stage/DEBIAN/conffiles: conffiles
	mkdir -p stage/DEBIAN
	cp conffiles stage/DEBIAN

stage/DEBIAN/control: control
	mkdir -p stage/DEBIAN
	cat control | sed "s/{{VERSION}}/${VERSION}/" > stage/DEBIAN/control

stage/etc/init/ddb.conf: upstart.conf
	mkdir -p stage/etc/init
	cp upstart.conf stage/etc/init/ddb.conf

prepare:
	mkdir -p stage/etc
	mkdir -p stage/usr/bin
	mkdir -p stage/usr/lib/ddb/lib
	mkdir -p stage/usr/share/ddb
	mkdir -p stage/var/lib/ddb
	mkdir -p stage/var/log/ddb
	cp -r ../../_build/deb/rel/ddb/bin stage/usr/lib/ddb/
	cp -r ../../_build/deb/rel/ddb/etc stage/etc/ddb
	cp -r ../../_build/deb/rel/ddb/erts-* stage/usr/lib/ddb/
	tar -cC ../../_build/deb/rel/ddb/lib --exclude c_src --exclude src . | tar -xC stage/usr/lib/ddb/lib
	cp -r ../../_build/deb/rel/ddb/releases stage/usr/lib/ddb/
	cp -r ../../_build/deb/rel/ddb/share stage/usr/lib/ddb/

clean:
	rm -rf ./stage
	rm -f *.deb
