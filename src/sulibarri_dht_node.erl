-module(sulibarri_dht_node).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, put/3, get/2, delete/2, cluster/0, cluster/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(ETS, ets_table).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% Do Active Checks here

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

put(Origin, Key, Value) ->
	Active = gen_server:call(sulibarri_dht_ring_manager, {active}),
	case Active of
		true ->
			gen_server:cast(?SERVER, {put, Origin, {Key, Value}});
		false ->
			{error, node_inactive}
	end.

get(Origin, Key) ->
	Active = gen_server:call(sulibarri_dht_ring_manager, {active}),
	case Active of
		true ->
			gen_server:cast(?SERVER, {get, Origin, Key});
		false ->
			{error, node_inactive}
	end.
	
delete(Origin, Key) ->
	Active = gen_server:call(sulibarri_dht_ring_manager, {active}),
	case Active of
		true ->
			gen_server:cast(?SERVER, {delete, Origin, Key});
		false ->
			{error, node_inactive}
	end.

cluster(Node) ->
	Active = gen_server:call(sulibarri_dht_ring_manager, {active}),
	case Active of
		true ->
			{error, node_active};
		false ->
			gen_server:call(?SERVER, {join, Node})
	end.
	%% get ring from node, stripe new ring
	%% init storage based on ring
	%% send ring

cluster() ->
	Active = gen_server:call(sulibarri_dht_ring_manager, {active}),
	case Active of
		true ->
			{error, node_active};			
		false ->
			gen_server:call(?SERVER, {cluster})

	end.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
	ets:new(?ETS, [named_table, protected]),
    {ok, []}.

handle_call({cluster}, _From, _State) ->
	Partition_List = sulibarri_dht_ring_manager:cluster(),
	lists:foreach(
		fun(P_Id) ->
			{ok, Pid} = sulibarri_dht_storage:create(P_Id),
			ets:insert(?ETS, {P_Id, Pid})
		end,
		Partition_List
	),
	{reply, ok, _State}.

% handle_call({join, Node}, _From, State) ->
% 	lager:info("Sending join request at ~p", [Node]),
% 	case nodes() of
% 	[] -> 
% 		case net_adm:ping(Node) of
% 			pang -> {reply, {error, not_reachable}, State};
% 			pong ->
% 				lager:info("Join request recieved, requesting data."),
% 				Data = gen_server:call({?SERVER, Node}, {request_join, node()}),
% 				sulibarri_dht_ets_store:bulk_insert(Data),
% 				{reply, ok, State}
% 		end;
% 	_ -> {reply, {error, already_clustered}, State}
% 	end;

% handle_call({request_join, Node}, _From, State) ->
% 	OldNodes = [X || X <- [node() | nodes()], X =/= Node],
% 	lists:foreach(
% 		fun(OldNode) ->
% 			gen_server:cast({?SERVER, OldNode}, {gossip, {joining, Node}})
% 		end,
% 		OldNodes
% 	),
% 	lager:info("Recieved data request from ~p, sending...", [Node]),
% 	Data = sulibarri_dht_ets_store:all(),
% 	{reply, Data, State}.
	
handle_cast({get, Origin, Key}, _State) ->
	Hash = sulibarri_dht_hash:hash(Key),
	Table = sulibarri_dht_ring_manager:partition_table(),
	{P_Id, {Node, _}} = sulibarri_dht_hash:lookup(Hash, Table),
	lager:info("Recieved get(~p), forwarding to ~p",[Key,Node]),
	gen_server:cast({?SERVER, Node}, {forward_get, P_Id, Hash, Origin}),
	{noreply, _State};

handle_cast({forward_get, P_Id, Hash, Origin}, _State) ->
	lager:info("Recieved get from ~p",[Origin]),
	case ets:lookup(?ETS, P_Id) of
		[{_, Pid}] ->
			Val = sulibarri_dht_storage:get(Pid, Hash);
		[] -> Val = not_found
	end,
	rpc:call(Origin, sulibarri_dht_client, output, [Val]),
	{noreply, _State};

handle_cast({put, Origin, {Key, Value}}, _State) ->
	Hash = sulibarri_dht_hash:hash(Key),
	Table = sulibarri_dht_ring_manager:partition_table(),
	{P_Id, {Node, _}} = sulibarri_dht_hash:lookup(Hash, Table),
	lager:info("Recieved put(~p,~p), forwarding to ~p",[Key, Value, Node]),
	gen_server:cast({?SERVER, Node}, {forward_put, P_Id, Hash, Key, Value, Origin}),
	{noreply, _State};

handle_cast({forward_put, P_Id, Hash, Key, Value, Origin}, _State) ->
	lager:info("Recieved put(~p, ~p) from ~p", [Key, Value, Origin]),

	case ets:lookup(?ETS, P_Id) of
		[{_, Pid}] ->
			sulibarri_dht_storage:put(Pid, Hash, {Key, Value}),
			{noreply, _State};
		[] ->
			lager:info("Put forwarded to wrong node!!!"),
			{noreply, _State}
	end.

% handle_cast({put, {Key, Value}}, State) ->
% 	Nodes = [node() | nodes()],
% 	lager:info("Broadcasting put(~p, ~p) to ~p.", [Key,Value,Nodes]),
% 	lists:foreach(
% 		fun(Node) ->
% 			gen_server:cast({?SERVER, Node}, {b_put, {Key, Value, node()}})
% 		end,
% 		Nodes
% 	),
% 	{noreply, State};

% handle_cast({delete, Key}, State) ->
% 	Nodes = [node() | nodes()],
% 	lager:info("Broadcasting delete(~p) to ~p.", [Key,Nodes]),
% 	lists:foreach(
% 		fun(Node) ->
% 			gen_server:cast({?SERVER, Node}, {b_delete, {Key, node()}})
% 		end,
% 		Nodes
% 	),
% 	{noreply, State};

% handle_cast({b_put, {Key, Value, Sender}}, State) ->
% 	lager:info("Recieved put(~p, ~p) from ~p.", [Key,Value,Sender]),
% 	sulibarri_dht_ets_store:put(Key, Value),
% 	{noreply, State};

% handle_cast({b_delete, {Key, Sender}}, State) ->
% 	lager:info("Recieved delete(~p) from ~p.", [Key,Sender]),
% 	sulibarri_dht_ets_store:delete(Key),
% 	{noreply, State};

% handle_cast({gossip, {_action, Node} = {Action, _}}, State) ->
% 	lager:info("~p ~p cluster.", [Node, Action]),
% 	{noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% erl -pa ebin deps/*/ebin -s lager -s sulibarri_dht_app -sname
% erl -pa ebin deps/*/ebin -s lager -s sulibarri_dht_app -name someName@(IP) -setcookie (someText)