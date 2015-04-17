-module(sulibarri_dht_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, start/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
	application:start(sulibarri_dht).

start(_StartType, _StartArgs) ->
	sulibarri_dht_ets_store:init(),
    sulibarri_dht_sup:start_link().

stop(_State) ->
    ok.
