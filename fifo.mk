REBAR = $(shell pwd)/rebar3
PKG_VSN = $(shell head -n1 rel/pkg/Makefile | sed 's/[^0-9.p]//g')
REBAR_VSN = $(shell grep 'release' rebar.config | sed 's/[^0-9.p]//g')
VARS_VSN = $(shell grep 'bugsnag_app_version' rel/vars.config | sed -e 's/.*,//' -e 's/[^0-9.p]//g' -e 's/[.]$$//')
APP_VSN = $(shell grep vsn apps/$(APP)/src/$(APP).app.src | sed 's/[^0-9.p]//g')

compile: $(REBAR) .git/hooks/pre-commit
	$(REBAR) compile

.git/hooks/pre-commit: hooks/pre-commit
	cp hooks/pre-commit .git/hooks

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
	make tree

update: $(REBAR)
	$(REBAR) update

tree: $(REBAR)
	$(REBAR) tree | grep -v '=' | sed 's/ (.*//' > tree

tree-diff: tree
	git diff test -- tree

###
### Docs
###
docs:
	$(REBAR) edoc

###
### Version
###

vsn: 
	@echo "## rel/pkg/Makefile"
	@echo "$(PKG_VSN)"
	@echo "## apps/$(APP)/src/$(APP).app.src"
	@echo "$(APP_VSN)"
	@echo "## rebar.config"
	@echo "$(REBAR_VSN)"
	@echo "## rel/vars.config"
	@echo "$(VARS_VSN)"

test-vsn:
	@echo "Testing against package version: $(PKG_VSN)"
	@[ "$(PKG_VSN)" == "$(APP_VSN)" ]   && echo " - App version ok:   $(APP_VSN)"   || (echo "App version out of date" && false)
	@[ "$(PKG_VSN)" == "$(REBAR_VSN)" ] && echo " - Rebar version ok: $(REBAR_VSN)" || (echo "Rebar version out of date" && false)
	@[ "$(PKG_VSN)" == "$(VARS_VSN)" ]  && echo " - Vars version ok:  $(VARS_VSN)"   || (echo "Vars version out of date" && false)
