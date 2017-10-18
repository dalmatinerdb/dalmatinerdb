APP=dalmatiner_db
.PHONY: all

REBAR=$(shell pwd)/rebar3
ELVIS=$(shell pwd)/elvis
APP=dalmatiner_db

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
