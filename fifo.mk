REBAR = $(shell pwd)/rebar3
PKG_VSN = $(shell head -n1 rel/pkg/Makefile | sed 's/[^0-9.p]//g')
REBAR_VSN = $(shell erl -noshell -eval '{ok, F} = file:consult("rebar.config"), [{release, {_, Vsn}, _}] = [O || {relx, [O | _]} <- F], io:format("~s", [Vsn]), init:stop().')
VARS_VSN = $(shell grep 'bugsnag_app_version' rel/vars.config | sed -e 's/.*,//' -e 's/[^0-9.p]//g' -e 's/[.]$$//')
APP_VSN = $(shell grep vsn apps/$(APP)/src/$(APP).app.src | sed 's/[^0-9.p]//g')

compile: $(REBAR) .git/hooks/pre-commit
	$(REBAR) compile

.git/hooks/pre-commit: hooks/pre-commit
	mkdir -p .git/hooks
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

update-fifo.mk:
	cp _build/default/lib/fifo_utils/priv/fifo.mk .


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
	@echo "## rel/pkg/Makefile"
	@echo "$(PKG_VSN)"
	@echo "## apps/$(APP)/src/$(APP).app.src"
	@echo "$(APP_VSN)"
	@echo "## rebar.config"
	@echo "$(REBAR_VSN)"
	@echo "## rel/vars.config"
	@echo "$(VARS_VSN)"

test-vsn:
	@echo "Testing against package version: $(REBAR_VSN)"
	@[ "$(REBAR_VSN)" = "$(APP_VSN)" ]  && echo " - App version ok:     $(APP_VSN)"  || (echo "App version out of date" && false)
	@[ "$(REBAR_VSN)" = "$(PKG_VSN)" ]  && echo " - Package version ok: $(PKG_VSN)"  || (echo "Package version out of date" && false)
	@[ "$(REBAR_VSN)" = "$(VARS_VSN)" ] && echo " - Vars version ok:    $(VARS_VSN)" || (echo "Vars version out of date" && false)
