-module(sulibarri_dht_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, infinity, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->

	%Supervisors = [sulibarri],


	Children = [?CHILD(sulibarri_dht_vnode_sup, supervisor),
				?CHILD(sulibarri_dht_vnode_router, worker),
				?CHILD(sulibarri_dht_put_fsm_sup, supervisor),
				?CHILD(sulibarri_dht_get_fsm_sup, supervisor),
				?CHILD(sulibarri_dht_node, worker),
				?CHILD(sulibarri_dht_ring_manager, worker)],

    {ok, { {one_for_one, 0, 1}, Children} }.

