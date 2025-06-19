export BUILD_WITHOUT_QUIC = 1

## Feature Used in rebar plugin emqx_plugrel
## The Feature have not enabled by default on OTP25
export ERL_FLAGS ?= -enable-feature maybe_expr

REBAR = $(CURDIR)/rebar3
SCRIPTS = $(CURDIR)/scripts

.PHONY: all
all: rel

.PHONY: ensure-rebar3
ensure-rebar3:
	@$(SCRIPTS)/ensure-rebar3.sh

$(REBAR):
	$(MAKE) ensure-rebar3

.PHONY: compile
compile: $(REBAR)
	$(REBAR) compile

.PHONY: ct
ct: $(REBAR)
	$(REBAR) as test ct -v --readable=true

.PHONY: eunit
eunit: $(REBAR)
	$(REBAR) as test eunit

.PHONY: cover
cover: $(REBAR)
	$(REBAR) cover

.PHONY: clean
clean: distclean

.PHONY: distclean
distclean:
	@rm -rf _build
	@rm -f rebar.lock

.PHONY: rel
rel: $(REBAR)
	$(REBAR) emqx_plugrel tar

## 'rebar3 fmt -w' does not work with sub-dir in src
.PHONY: fmt
fmt: $(REBAR)
	@find . \( -name '*.app.src' -o \
	-name '*.erl' -o \
	-name '*.hrl' -o \
	-name 'rebar.config' -o \
	-name '*.eterm' -o \
	-name '*.escript' \) \
	-not -path '*/_build/*' \
	-type f | xargs $(SCRIPTS)/erlfmt -w

.PHONY: fmt-check
fmt-check: $(REBAR)
	$(REBAR) fmt --verbose --check

.PHONY: run
run: rel
	./scripts/run.sh emqx/emqx-enterprise:5.10.0-gf1c14e70

.PHONY: run-cleanup
run-cleanup:
	./scripts/run.sh -c
