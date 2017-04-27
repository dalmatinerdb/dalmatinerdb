APP=dalmatiner_db
.PHONY: all tree

all: compile

include fifo.mk

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

deb-package: deb-prepare
	make -C rel/deb package

dummy:
	true
