FIFO_APP=dalmatiner_db
FIFO_APP_HOME="/data/dalmatinerdb"
FIFO_APP_VERSION="$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)"

configfiles=share/ddb.xml rel/pkg/displayfile rel/pkg/install.sh rel/vars.config
.PHONY: all version_header tree clean rel package deb-clean deb-prepare dummy

all: $(FIFO_APP).version $(configfiles) compile

version_header: apps/$(FIFO_APP)/include/$(FIFO_APP)_version.hrl # needed by rebar (see rebar.config)

include fifo.mk

$(FIFO_APP).version:
	@echo $(FIFO_APP_VERSION) > $@

apps/$(FIFO_APP)/include/$(FIFO_APP)_version.hrl: $(FIFO_APP).version
	@echo "-define(VERSION, <<\"$(shell cat $<)\">>)." > $@

$(configfiles):
	sed 's,/data/dalmatinerdb,$(FIFO_APP_HOME),g' $@.tmpl > $@

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
