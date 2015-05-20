%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_vnode_sup).

-behaviour(supervisor).


%% API
-export([start_link/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(VNodeId) ->
	supervisor:start_child(?MODULE, [VNodeId]).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->

	VNode = {sulibarri_dht_vnode, {sulibarri_dht_vnode, start_link, []},
				transient, infinity, worker, [sulibarri_dht_vnode]},

    {ok, { {simple_one_for_one, 0, 1}, [VNode]} }.

