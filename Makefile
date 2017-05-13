APP=dalmatiner_db
.PHONY: all tree

REBAR=$(shell pwd)/rebar3
ELVIS=$(shell pwd)/elvis
APP=sniffle

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_V6 := $(shell sh -c 'uname -v 2>/dev/null | cut -c-6 || echo not')


ifeq ($(uname_S),FreeBSD)
        PLATFORM = freebsd
        WMAKE = gmake
        REBARPROFILE = fbsd
        export REBARPROFILE
endif
ifeq ($(uname_V6),joyent)
        PLATFORM = smartos
        WMAKE = make
        REBARPROFILE = smartos
        export REBARPROFILE
endif

include fifo.mk

clean:
	$(REBAR) clean
	make -C rel/pkg clean
	make -C rel/deb clean
	make -C rel/pkgng clean


## LEAVING THIS IN FOR NOW
## TODO CONFORM WITH OTHER BUILD CMDS
deb-clean: 
	make -C rel/deb clean

deb-prepare:
	$(REBAR) as deb compile
	$(REBAR) as deb release
	make -C rel/deb prepare

deb-package: deb-prepare
	make -C rel/deb package
##



version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > dalmatinerdb.version

version_header: version
	true
	#@echo "-define(VERSION, <<\"$(shell cat dalmatinerdb.version)\">>)." > apps/dalmatinerdb/include/dalmatinerdb.hrl

long-test:
	$(REBAR) as eqc,long eunit

dist: ${PLATFORM} ;

freebsd: update ${PLATFORM}/rel
	gmake -C rel/pkgng package

smartos: update ${PLATFORM}/rel
	make -C rel/pkg package

clean:
	$(REBAR) clean
	make -C rel/pkg clean
	make -C rel/pkgng clean

freebsd/rel: version_header
	$(REBAR) as ${REBARPROFILE} compile
	$(REBAR) as ${REBARPROFILE} release

smartos/rel: version_header
	$(REBAR) as ${REBARPROFILE} compile
	$(REBAR) as ${REBARPROFILE} release
