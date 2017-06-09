REBAR=$(shell which rebar3)

all: compile

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

ct:
	@$(REBAR) ct

eunit:
	@$(REBAR) eunit

test: eunit ct

