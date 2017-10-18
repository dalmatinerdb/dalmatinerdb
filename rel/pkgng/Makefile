INSTALL_DIR=usr/local/lib
DEPS="coreutils" "sudo" "bash"
REBARPROFILE ?= default

include ../../config.mk
include ../../_build/${REBARPROFILE}/lib/fifo_utils/priv/pkgng.mk

.PHONY: package prepare clean

prepare:
	-rm -r $(STAGE_DIR)/$(INSTALL_DIR)/$(COMPONENT_INTERNAL)
	-rm  $(STAGE_DIR)/+*
	-rm  $(STAGE_DIR)/plist
	mkdir -p $(STAGE_DIR)/$(INSTALL_DIR)
	cp -r ../../_build/${REBARPROFILE}/rel/$(COMPONENT_INTERNAL) $(STAGE_DIR)/$(INSTALL_DIR)/$(COMPONENT_INTERNAL)
	rm -rf $(STAGE_DIR)/$(INSTALL_DIR)/$(COMPONENT_INTERNAL)/lib/*/c_src
