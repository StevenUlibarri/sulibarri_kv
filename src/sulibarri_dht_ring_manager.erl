%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_ring_manager).
-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(RING_PATH, "storage/" ++ atom_to_list(node()) ++ "/ring.ring").



%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-compile([export_all]).

% -export([
%         start_link/0,
%         partition_table/0,
%         state/0,
%         new_cluster/2,
%         join_cluster/1
%         ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_ring_state() ->
	gen_server:call(?SERVER, get_state).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    case sulibarri_dht_ring:read_ring(?RING_PATH) of
        not_found ->
            lager:warning("No Ring file found, creating new one"),
            State = sulibarri_dht_ring:new_ring([node()]),
            sulibarri_dht_ring:write_ring(?RING_PATH, State);
        State -> ok
    end,
    VNodes = sulibarri_dht_ring:get_vnodes_for_node(node(), State),
    lists:foreach(
        fun({_, VNode_Id}) ->
            sulibarri_dht_vnode:create(VNode_Id)
        end,
        VNodes
    ),
    {ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

