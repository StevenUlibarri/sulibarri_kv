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
	case filelib:ensure_dir("storage/" ++ atom_to_list(node()) ++ "/") of
		ok ->
			sulibarri_dht_sup:start_link();
		{error, Reason} ->
			lager:critical("Could not find or create node directory"),
			throw({error, invalid_node_directory})
	end.
    

stop(_State) ->
    ok.
