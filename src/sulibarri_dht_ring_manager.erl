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

get_nodes() ->
    gen_server:call(?SERVER, nodes).

new_cluster(Nodes) ->
    gen_server:cast(?SERVER, {new_cluster, Nodes}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    case sulibarri_dht_ring:read_ring(?RING_PATH) of
        not_found ->
            lager:warning("No ring file"),
            Ring = undefined;
            % lager:warning("No Ring file found, creating new one"),
            % State = sulibarri_dht_ring:new_ring([node()]),
            % sulibarri_dht_ring:write_ring(?RING_PATH, State);
        Ring -> 
            Ids = get_Vnode_Ids(Ring),
            Nodes = sulibarri_dht_ring:get_nodes(Ring),
            sulibarri_dht_vnode_router:start_vnode(lists:reverse(Ids)),
            sulibarri_dht_node_watcher:node_up(node(), Nodes),
            sulibarri_dht_node_watcher:start_monitor(Nodes)
    end,
    {ok, Ring}.

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(nodes, _From, State) ->
    Nodes = sulibarri_dht_ring:get_nodes(State),
    {reply, Nodes, State}.


handle_cast({new_cluster, Nodes}, State) ->
    Ring = sulibarri_dht_ring:new_ring(Nodes),
    %% abCast to cluster new ring
    Nodes = sulibarri_dht_ring:get_nodes(Ring),
    gen_server:abcast(Nodes, ?SERVER, {new_ring, Ring}),
    {noreply, State};

handle_cast({new_ring, Ring}, _State) ->
    sulibarri_dht_ring:write_ring(?RING_PATH, Ring),
    Ids = get_Vnode_Ids(Ring),
    sulibarri_dht_vnode_router:start_vnode(lists:reverse(Ids)),
    Nodes = sulibarri_dht_ring:get_nodes(Ring),
    sulibarri_dht_node_watcher:start_monitor(Nodes),
    {noreply, Ring}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

get_Vnode_Ids(Ring) ->
    VNodes = sulibarri_dht_ring:get_vnodes_for_node(node(), Ring),
    Ids = lists:foldl(
        fun({_, VNode_Id, _}, Acc) ->
            [VNode_Id | Acc]
        end,
        [],
        VNodes
    ),
    Ids.