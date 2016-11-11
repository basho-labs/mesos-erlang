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

protoclean:
	-rm -rf $(BASE_DIR)/proto
	-rm $(BASE_DIR)/include/*_protobuf.hrl
	-rm $(BASE_DIR)/src/*_protobuf.erl

distclean: clean
	$(REBAR) delete-deps

test: deps compile
	$(REBAR) ct

docker:
	./test/erl_mesos_cluster/cluster.sh build
