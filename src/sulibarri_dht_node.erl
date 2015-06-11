-module(sulibarri_dht_node).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
		put/3,
		get/2,
		delete/2,

		new_cluster/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% Do Active Checks here

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

put(Key, Value, Origin) ->
	gen_server:cast(?SERVER, {put, Key, Value, Origin}).

get(Key, Origin) ->
	gen_server:cast(?SERVER, {get, Key, Origin}).
	
delete(Key, Origin) ->
	gen_server:cast(?SERVER, {delete, Key, Origin}).

new_cluster(Nodes) ->
	gen_server:cast(?SERVER, {new_cluster, Nodes}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({new_cluster, Nodes}, State) ->
	case check_nodes(Nodes) of
		{error, Unreachable_Nodes} -> 
			lager:error("Nodes, Unreachable: ~p", [Unreachable_Nodes]);
		ok ->
			sulibarri_dht_ring_manager:new_cluster(Nodes)
	end,
	{noreply, State};

handle_cast({put, Obj, Origin}, State) ->
	sulibarri_dht_put_fsm:create(Obj, Origin),
	{noreply, State};

handle_cast({get, Key, Origin}, State) ->
	sulibarri_dht_get_fsm:create(Key, Origin),
	{noreply, State}.

% handle_cast({init_cluster, VNodes}, State) ->
% 	lager:info("Initializing VNodes"),
% 	lists:foreach(
% 		fun({_, VNodeId}) ->
% 			sulibarri_dht_vnode:create(VNodeId) end,
% 		VNodes
% 	),
% 	lager:info("~p Vnodes Initialized", [length(VNodes)]),
% 	{noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% PRIVATE %%

check_nodes(Nodes) ->
	Unreachable_Nodes = lists:foldl(
		fun(Node, Acc) ->
			case net_adm:ping(Node) of
				pang -> [Node | Acc];
				pong -> Acc
			end
		end,
		[],
		Nodes
	),
	case length(Unreachable_Nodes) of
		0 -> ok;
		_ -> {error, Unreachable_Nodes}
	end.