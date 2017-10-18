REBAR = $(shell pwd)/rebar3
REBAR_VSN = $(shell erl -noshell -eval '{ok, F} = file:consult("rebar.config"), [{release, {_, Vsn}, _}] = [O || {relx, [O | _]} <- F], io:format("~s", [Vsn]), init:stop().')
VARS_VSN = $(shell grep 'bugsnag_app_version' rel/vars.config | sed -e 's/.*,//' -e 's/[^0-9.p]//g' -e 's/[.]$$//')
APP_VSN = $(shell grep vsn apps/$(APP)/src/$(APP).app.src | sed 's/[^0-9.p]//g')

include config.mk

compile: $(REBAR) .git/hooks/pre-commit
	$(REBAR) compile

.git/hooks/pre-commit: hooks/pre-commit
	[ -f  .git/hooks ] && cp hooks/pre-commit .git/hooks || true

pre-commit: test-scripts test-vsn lint xref dialyzer test

dialyzer: $(REBAR)
	$(REBAR) dialyzer

xref: $(REBAR)
	$(REBAR) xref

test-scripts:
	for i in rel/files/*; do (head -1 $$i | grep -v sh > /dev/null) || bash -n $$i || exit 1; done;

test: $(REBAR)
	$(REBAR) eunit

lint: $(REBAR)
	$(REBAR) as lint lint

$(REBAR):
	cp `which rebar3` $(REBAR)

upgrade: $(REBAR)
	$(REBAR) upgrade
	$(MAKE) tree

update: $(REBAR)
	$(REBAR) update

rebar.lock: rebar.config $(REBAR)
	$(REBAR) compile

tree: $(REBAR) rebar.lock
	$(REBAR) tree | grep -v '=' | sed 's/ (.*//' > tree

tree-diff: tree
	git diff test -- tree

update-fifo.mk:
	cp _build/default/lib/fifo_utils/priv/fifo.mk .

###
### Packaging
###

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_V6 := $(shell sh -c 'uname -v 2>/dev/null | cut -c-6 || echo not')

ifeq ($(uname_S),Darwin)
	PLATFORM = darwin
	REBARPROFILE = darwin
	export REBARPROFILE
endif
ifeq ($(uname_S),FreeBSD)
	PLATFORM = freebsd
	REBARPROFILE = freebsd
	export REBARPROFILE
endif
ifeq ($(uname_V6),joyent)
	PLATFORM = smartos
	REBARPROFILE = smartos
	export REBARPROFILE
endif

dist: ${PLATFORM} ;

generic/rel: version_header
	$(REBAR) as ${REBARPROFILE} compile
	$(REBAR) as ${REBARPROFILE} release

freebsd: ${PLATFORM}/rel
	$(MAKE) -C rel/pkgng package

smartos:  ${PLATFORM}/rel
	$(MAKE) -C rel/pkg package

darwin:  ${PLATFORM}/rel

freebsd/rel: generic/rel

smartos/rel: generic/rel

darwin/rel: generic/rel

dist-help:
	@echo "FiFo dist tool"
	@echo "You are running this on: ${PLATFORM}"
	@echo
	@echo "Currently supported platforms are: FreeBSD, SmartOS, Darwin/OSX"
	@echo
	@echo "SmartOS:"
	@echo " rebar profile: smartos $(shell if grep profiles -A12 rebar.config | grep smartos > /dev/null; then echo OK; else echo MISSING; fi)"
	@echo " packaging makefile: rel/pkg/Makefile $(shell if [ -f rel/pkg/Makefile ]; then echo OK; else echo MISSING; fi)"
	@echo "FreeBSD:"
	@echo " rebar profile: freebsd $(shell if grep profiles -A12 rebar.config | grep freebsd > /dev/null; then echo OK; else echo MISSING; fi)"
	@echo " packaging makefile: rel/pkgng/Makefile $(shell if [ -f rel/pkgng/Makefile ]; then echo OK; else echo MISSING; fi)"
	@echo "Darwin:"
	@echo " rebar profile: darwin $(shell if grep profiles -A12 rebar.config | grep darwin > /dev/null; then echo OK; else echo MISSING; fi)"
	@echo " packaging makefile: - no packaing -"

###
### Docs
###
docs:
	$(REBAR) edoc

###
### Version
###

build-vsn:
	@echo "$(REBAR_VSN)"
vsn:
	@echo "## Config:"
	@echo "$(VERSION)"
	@echo "## apps/$(APP)/src/$(APP).app.src"
	@echo "$(APP_VSN)"
	@echo "## rebar.config"
	@echo "$(REBAR_VSN)"
	@echo "## rel/vars.config"
	@echo "$(VARS_VSN)"

test-vsn:
	@echo "Testing against package version: $(VERSION)"
	@[ "$(VERSION)" = "$(APP_VSN)" ]   && echo " - App version ok:   $(APP_VSN)"   || (echo "App version out of date" && false)
	@[ "$(VERSION)" = "$(REBAR_VSN)" ] && echo " - Rebar version ok: $(REBAR_VSN)" || (echo "Package version out of date" && false)
	@[ "$(VERSION)" = "$(VARS_VSN)" ]  && echo " - Vars version ok:  $(VARS_VSN)"  || (echo "Vars version out of date" && false)
