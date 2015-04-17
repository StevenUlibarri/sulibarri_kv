-module(sulibarri_dht_node).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, put/2, get/1, delete/1, join/1]).

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

put(Key, Value) ->
	gen_server:cast(?SERVER, {put, {Key, Value}}).

get(Key) ->
	gen_server:call(?SERVER, {get, Key}).

delete(Key) ->
	gen_server:cast(?SERVER, {delete, Key}).

join(Node) ->
	gen_server:call(?SERVER, {join, Node}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call({get, Key}, _From, _State) ->
	try
		{ok, Value} = sulibarri_dht_ets_store:get(Key),
		{reply, Value, _State}
	catch
		_Class:_Exception ->
			%{error, not_found},
			{reply, not_found, _State}
	end;

handle_call({join, Node}, _From, State) ->
	lager:info("Sending join request at ~p", [Node]),
	case nodes() of
	[] -> 
		case net_adm:ping(Node) of
			pang -> {reply, {error, not_reachable}, State};
			pong ->
				lager:info("Join request recieved, requesting data."),
				Data = gen_server:call({?SERVER, Node}, {request_join, node()}),
				sulibarri_dht_ets_store:bulk_insert(Data),
				{reply, ok, State}
		end;
	_ -> {reply, {error, already_clustered}, State}
	end;

handle_call({request_join, Node}, _From, State) ->
	OldNodes = [X || X <- [node() | nodes()], X =/= Node],
	lists:foreach(
		fun(OldNode) ->
			gen_server:cast({?SERVER, OldNode}, {gossip, {joining, Node}})
		end,
		OldNodes
	),
	lager:info("Recieved data request from ~p, sending...", [Node]),
	Data = sulibarri_dht_ets_store:all(),
	{reply, Data, State}.
	
handle_cast({put, {Key, Value}}, State) ->
	Nodes = [node() | nodes()],
	lager:info("Broadcasting put(~p, ~p) to ~p.", [Key,Value,Nodes]),
	lists:foreach(
		fun(Node) ->
			gen_server:cast({?SERVER, Node}, {b_put, {Key, Value, node()}})
		end,
		Nodes
	),
	{noreply, State};

handle_cast({delete, Key}, State) ->
	Nodes = [node() | nodes()],
	lager:info("Broadcasting delete(~p) to ~p.", [Key,Nodes]),
	lists:foreach(
		fun(Node) ->
			gen_server:cast({?SERVER, Node}, {b_delete, {Key, node()}})
		end,
		Nodes
	),
	{noreply, State};

handle_cast({b_put, {Key, Value, Sender}}, State) ->
	lager:info("Recieved put(~p, ~p) from ~p.", [Key,Value,Sender]),
	sulibarri_dht_ets_store:put(Key, Value),
	{noreply, State};

handle_cast({b_delete, {Key, Sender}}, State) ->
	lager:info("Recieved delete(~p) from ~p.", [Key,Sender]),
	sulibarri_dht_ets_store:delete(Key),
	{noreply, State};

handle_cast({gossip, {_action, Node} = {Action, _}}, State) ->
	lager:info("~p ~p cluster.", [Node, Action]),
	{noreply, State}.

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