-module(sulibarri_dht_put_fsm).


-export([start_link/0]).

-behaviour(gen_fsm).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3, handle_sync_event/4]).

-compile([export_all]).


-define(SERVER, ?MODULE).

start_link() ->
	gen_fsm:start_link({local, ?SERVER}, ?MODULE, {}, []).

%% @private
init({}) ->
	{ok, validate, state, 0}.

%% @private
validate(timeout, State) ->
	{next_state, execute_local, State, 0}.

%% @private
execute_local(timeout, State) ->
	{next_state, waiting_local, State}.

%% @private
waiting_local(request_timeout, State) ->
	{next_state, execute, State}.

handle_event(_Event, _From, _State)->
	{reply, ok, _State}.

%% @private
handle_sync_event(_Event, _From, StateName, State) ->
	{reply, ok, StateName, State}.

%% @private
handle_info(_Info, StateName, State) ->
	{next_state, StateName, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
	ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
	{ok, StateName, State}.