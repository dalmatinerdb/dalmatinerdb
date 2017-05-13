VERSION=0.3.0
COMPONENT_INTERNAL=ddb
INSTALL_DIR=usr
DEPS="coreutils" "sudo"

include ../../_build/${REBARPROFILE}/lib/fifo_utils/priv/pkgng.mk

.PHONY: package prepare clean

prepare:
	-rm -r $(STAGE_DIR)
	mkdir -p $(STAGE_DIR)/$(INSTALL_DIR)
	cp -r ../../_build/${REBARPROFILE}/rel/$(COMPONENT_INTERNAL) $(STAGE_DIR)/$(INSTALL_DIR)
	rm -rf $(STAGE_DIR)/$(INSTALL_DIR)/lib/*/c_src
	mkdir -p $(STAGE_DIR)/etc/rc.d
	cp ../freebsd/dalmatinerdb $(STAGE_DIR)/etc/rc.d/

package: prepare dep_list package_list
	cat +MANIFEST deplist > $(STAGE_DIR)/+MANIFEST
	cp plist $(STAGE_DIR)/
	cp +POST_INSTALL $(STAGE_DIR)/
	pkg create -m $(STAGE_DIR)/ -r $(STAGE_DIR)/ -p $(STAGE_DIR)/plist -o .


clean: clean-pkg
	-rm -rf deploy
	-rm *.tgz
