DEPS="erlang" "coreutils" "sudo" "grep" "zlib" "bzip2" "gcc49-libs"

include ../../config.mk
include ../../_build/${REBARPROFILE}/lib/fifo_utils/priv/pkg.mk

.PHONY: package prepare clean

prepare:
	-rm -r $(STAGE_DIR)/$(COMPONENT)
	cp -r ../../_build/${REBARPROFILE}/rel/$(COMPONENT_INTERNAL) $(STAGE_DIR)/$(COMPONENT)
	rm -rf $(STAGE_DIR)/$(COMPONENT)/lib/*/c_src

package: prepare $(FILE).tgz

clean: clean-pkg
	-rm *.tgz
