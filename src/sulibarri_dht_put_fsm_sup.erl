%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_put_fsm_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1, start_child/2]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, temporary, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Obj, Origin) ->
	supervisor:start_child(?MODULE, [Obj, Origin]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->

	Put_Fsm = {sulibarri_dht_put_fsm, {sulibarri_dht_put_fsm, start_link, []},
				temporary, 5000, worker, [sulibarri_dht_put_fsm]},

    {ok, { {simple_one_for_one, 5, 10}, [Put_Fsm]} }.

