APP=dalmatiner_db
.PHONY: all tree

all: version compile

include fifo.mk

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > dalmatiner_db.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat dalmatiner_db.version)\">>)." > apps/dalmatiner_db/include/dalmatiner_db_version.hrl

clean:
	$(REBAR) clean
	make -C rel/pkg clean
	make -C rel/deb clean

rel: dummy
	$(REBAR) as prod release

package: rel
	make -C rel/pkg package

deb-clean: 
	make -C rel/deb clean

deb-prepare:
	$(REBAR) as deb compile
	$(REBAR) as deb release
	make -C rel/deb prepare

dummy:
	true
