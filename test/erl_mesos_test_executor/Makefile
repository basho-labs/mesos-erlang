BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/../../rebar

.PHONY: deps

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

relclean:
	-rm -rf rel/erl_mesos_test_executor

rel: clean relclean compile
	$(REBAR) generate
