%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_storage_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

%% API
-export([start_link/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Partition_Number) ->
	supervisor:start_child(?SERVER, [Partition_Number]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	Partition_Store = {sulibarri_dht_storage, {sulibarri_dht_storage, start_link, []},
						transient, infinity, worker, [sulibarri_dht_storage]},

	Children = [Partition_Store],

    {ok, { {simple_one_for_one, 0, 1}, Children} }.

