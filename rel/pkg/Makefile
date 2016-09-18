VERSION=0.2.1
COMPONENT_INTERNAL=ddb
COMPONENT=dalmatinerdb
DEPS="erlang" "coreutils" "sudo" "grep" "zlib" "bzip2" "gcc47-libs"

include ../../_build/default/lib/fifo_utils/priv/pkg.mk

.PHONY: prepare

prepare:
	-rm -r $(STAGE_DIR)/$(COMPONENT)
	cp -r ../../_build/prod/rel/$(COMPONENT_INTERNAL) $(STAGE_DIR)/$(COMPONENT)
	rm -rf $(STAGE_DIR)/$(COMPONENT)/lib/*/c_src


package: prepare $(FILE).tgz

clean: clean-pkg
	-rm *.tgz
