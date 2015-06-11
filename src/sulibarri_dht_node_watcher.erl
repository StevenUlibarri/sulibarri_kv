%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_node_watcher).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        node_up/2,
        start_monitor/1
        ]).

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

node_up(Node, Nodes) ->
	% Nodes = sulibarri_dht_ring_manager:get_nodes(),
	gen_server:abcast(Nodes -- [node()], ?SERVER, {node_up, Node}).

start_monitor(Nodes) ->
    gen_server:cast(?SERVER, {start_monitor, Nodes}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({node_up, Node}, State) ->
	lager:info("Node ~p up", [Node]),
	sulibarri_dht_vnode_router:check_handoff_for_node(Node),
	{noreply, State};

handle_cast({start_monitor, Nodes}, _State) ->
    lager:notice("Node Monitor Started"),
    Watch_List = lists:foldl(
        fun(Node, Acc) ->
            case net_adm:ping(Node) of
                pong -> 
                    sulibarri_dht_vnode_router:check_handoff_for_node(Node),
                    [{Node, pong} | Acc];
                pang -> [{Node, pang} | Acc]
            end
        end,
        [],
        Nodes
    ),
    erlang:send_after(1000, self(), check),
    {noreply, Watch_List};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, Watch_List) ->
    Updated_Watch_List = lists:foldl(
        fun({Node, Status}, Acc) ->
            case net_adm:ping(Node) of
                pong when Status =:= pang ->
                    gen_server:cast(?SERVER, {node_up, Node}),
                    [{Node, pong} | Acc];
                pang when Status =:= pong ->
                    [{Node, pang} | Acc];
                _ ->
                    [{Node, Status}| Acc]
            end
        end,
        [],
        Watch_List
    ),
    erlang:send_after(3000, self(), check),
    {noreply, Updated_Watch_List}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

