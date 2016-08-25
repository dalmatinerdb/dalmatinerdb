VERSION = $(BUILD_NUMBER) # THis does not seem to work at the moment
VERSION ?= 0
COMPONENT = ddb

.PHONY: prepare package

prepare:
	mkdir -p stage/etc
	mkdir -p stage/etc/systemd/system
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
	cp -r systemd/ddb.service stage/etc/systemd/system/ddb.service

clean:
	rm -rf ./stage
	rm -f *.deb
