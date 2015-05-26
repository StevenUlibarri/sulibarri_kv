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
		{error, Reason} -> lager:info("New Cluster failed with: ~p", [Reason]);
		ok ->
			sulibarri_dht_ring_manager:new_cluster(Nodes)
	end,
	{noreply, State};

handle_cast({init_cluster, VNodes}, State) ->
	lager:info("Initializing VNodes"),
	lists:foreach(
		fun({_, VNodeId}) ->
			sulibarri_dht_vnode:create(VNodeId) end,
		VNodes
	),
	lager:info("Vnodes Initialized"),
	{noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% PRIVATE %%

check_nodes(Nodes) ->
	lists:foreach(
		fun(Node) ->
			case net_adm:ping(Node) of
				pang -> {error, {Node, unreachable}};
				pong -> ok
			end
		end,
		Nodes
	),
	ok.

% handle_cast({join_cluster, Node}, State) ->
% 	sulibarri_dht_ring_manager:join_cluster(Node),
% 	{noreply, State};

% handle_cast({init_transfers, Transfers}, State) ->
% 	% lists:foreach(
% 	% 	fun({P_Id, _, Destination}) ->
% 	% 		case ets:lookup(?ETS, P_Id) of
% 	% 			[{_, Pid}] ->
% 	% 				sulibarri_dht_storage:init_transfer(Pid, {Destination});
% 	% 			[] -> nop
% 	% 		end
% 	% 	end,
% 	% 	Transfers
% 	% ),
% 	{noreply, State};

% handle_cast({revieve_transfer, P_Id, Data, Sender}, State) ->
% 	% [{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	% sulibarri_dht_storage:receive_transfer(Pid, Data, Sender),
% 	{noreply, State};

% handle_cast({put, Key, Value, Origin}, State) ->
% 	Hash = sulibarri_dht_ring:hash(Key),
% 	Table = sulibarri_dht_ring_manager:partition_table(),
% 	{P_Id, {Node, _}} = sulibarri_dht_ring:lookup(Hash, Table),

% 	case Node =:= node() of
% 		true ->
% 			lager:info("Recieved Put(~p,~p); Node is primary, coordinating...",
% 						[Key, Value]),
% 			[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 			replicate_put(P_Id, Hash, Key, Value, node()),
% 			sulibarri_dht_storage:put(Pid, Hash, {Key, Value}, Origin);
% 		false ->
% 			lager:info("Recieved Put(~p,~p); Node is not primary, forwarding to ~p",
% 						[Key, Value, Node]),
% 			gen_server:cast({?SERVER, Node},
% 						{forward_put, P_Id, Hash, Key, Value, Origin})
% 	end,
% 	{noreply, State};

% handle_cast({get, Key, Origin}, State) ->
% 	Hash = sulibarri_dht_ring:hash(Key),
% 	Table = sulibarri_dht_ring_manager:partition_table(),
% 	{P_Id, {Node, _}} = sulibarri_dht_ring:lookup(Hash, Table),

% 	case Node =:= node() of
% 		true ->
% 			lager:info("Recieved Get(~p); Node is primary, coordinating...", [Key]),
% 			[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 			sulibarri_dht_storage:get(Pid, Hash, Origin);
% 		false ->
% 			lager:info("Recieved Get(~p); Node is not primary, forwarding to ~p",
% 						[Key,Node]),
% 			gen_server:cast({?SERVER, Node},
% 						{forward_get, P_Id, Hash, Key, Origin})
% 	end,
% 	{noreply, State};

% handle_cast({delete, Key, Origin}, State) ->
% 	Hash = sulibarri_dht_ring:hash(Key),
% 	Table = sulibarri_dht_ring_manager:partition_table(),
% 	{P_Id, {Node, _}} = sulibarri_dht_ring:lookup(Hash, Table),

% 	case Node =:= node() of
% 		true ->
% 			lager:info("Recieved Delete(~p); Node is primary, coordinating...", [Key]),
% 			[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 			replicate_delete(P_Id, Hash, Key, node()),
% 			sulibarri_dht_storage:delete(Pid, Hash, Origin);
% 		false ->
% 			lager:info("Recieved Delete(~p); Node is not primary, forwarding to ~p",
% 						[Key,Node]),
% 			gen_server:cast({?SERVER, Node},
% 						{forward_delete, P_Id, Hash, Key, Origin})
% 	end,
% 	{noreply, State};

% handle_cast({forward_put, P_Id, Hash, Key, Value, Origin}, State) ->
% 	lager:info("Recieved forwarded Put(~p,~p); Node is primary, coordinating...",
% 						[Key,Value]),
% 	[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	replicate_put(P_Id, Hash, Key, Value, node()),
% 	sulibarri_dht_storage:put(Pid, Hash, {Key, Value}, Origin),
% 	{noreply, State};

% handle_cast({forward_get, P_Id, Hash, Key, Origin}, State) ->
% 	lager:info("Recieved forwarded get(~p); Node is primary, coordinating...",
% 				[Key]),
% 	[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	sulibarri_dht_storage:get(Pid, Hash, Origin),
% 	{noreply, State};

% handle_cast({forward_delete, P_Id, Hash, Key, Origin}, State) ->
% 	lager:info("Recieved forwarded delete(~p); Node is primary, coordinating...",
% 				[Key]),
% 	[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	replicate_delete(P_Id, Hash, Key, node()),
% 	sulibarri_dht_storage:delete(Pid, Hash, Origin),
% 	{noreply, State};

% handle_cast({replicate_put, P_Id, Hash, Key, Value, Sender}, State) ->
% 	lager:info("Recieved replicate Put(~p,~p) from ~p; Peristing...",
% 						[Key,Value,Sender]),
% 	[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	sulibarri_dht_storage:put(Pid, Hash, {Key, Value}, Sender),
% 	{noreply, State};

% handle_cast({replicate_delete, P_Id, Hash, Key, Sender}, State) ->
% 	lager:info("Recieved replicate Delete(~p) from ~p; Persisting...",[Key,Sender]),
% 	[{_, Pid}] = ets:lookup(?ETS, P_Id),
% 	sulibarri_dht_storage:delete(Pid, Hash, Sender),
% 	{noreply, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% erl -pa ebin deps/*/ebin -s lager -s sulibarri_dht_app -sname
% erl -pa ebin deps/*/ebin -s lager -s sulibarri_dht_app -name someName@(IP) -setcookie (someText)

% sulibarri_dht_client:join_cluster(nodeA@localhost).

% replicate_put(P_Id, Hash, Key, Value, Sender) ->
% 	#state{partition_table = Table, n_val = N_Val} = sulibarri_dht_ring_manager:state(),
% 	Pref_List = sulibarri_dht_ring:get_pref_list(P_Id, Table, N_Val),
% 	Primaries = proplists:get_value(primaries, Pref_List),

% 	gen_server:abcast(Primaries, ?SERVER, {replicate_put, P_Id, Hash, Key, Value, Sender}).


% replicate_delete(P_Id, Hash, Key, Sender) ->
% 	#state{partition_table = Table, n_val = N_Val} = sulibarri_dht_ring_manager:state(),
% 	Pref_List = sulibarri_dht_ring:get_pref_list(P_Id, Table, N_Val),
% 	Primaries = proplists:get_value(primaries, Pref_List),

% 	gen_server:abcast(Primaries, ?SERVER, {replicate_delete, P_Id, Hash, Key, Sender}).
