%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_get_fsm_sup).

-behaviour(supervisor).

-define(SERVER, ?MODULE).

%% API
-export([start_link/0, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, temporary, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Key, Origin) ->
	supervisor:start_child(?MODULE, [Key, Origin]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->

	Get_Fsm = ?CHILD(sulibarri_dht_get_fsm, worker),

    {ok, { {simple_one_for_one, 5, 10}, [Get_Fsm]} }.

