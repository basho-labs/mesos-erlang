BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar

.PHONY: deps

all: deps compile

compile:
	$(REBAR) compile

recompile:
	$(REBAR) compile skip_deps=true

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

distclean: clean
	$(REBAR) delete-deps

test: deps compile
	$(REBAR) ct

docker:
	./test/erl_mesos_cluster/cluster.sh build
