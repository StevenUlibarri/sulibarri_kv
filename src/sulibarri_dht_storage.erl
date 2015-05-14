%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_storage).
-behaviour(gen_server).

-define(SERVER, ?MODULE).
-define(ETS(X), (list_to_atom("p" ++ integer_to_list(X)))).

-record(state, {partition_number,ets_name}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/1,
        create/1,
        init_transfer/2,
        receive_transfer/3,
        destroy/0,
        put/4,
        get/3,
        delete/3
        ]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Partition_Number) ->
    gen_server:start_link(?MODULE, [Partition_Number], []).

create(Partition_Number) ->
	sulibarri_dht_storage_sup:start_child(Partition_Number).

init_transfer(Pid, Destination_Node) ->
	gen_server:cast(Pid, {init_transfer, Destination_Node}).

receive_transfer(Pid, Data, Sender) ->
	gen_server:cast(Pid, {receive_transfer, Data, Sender}).

destroy() ->
	kill.

put(Pid, Key, Value, Origin) ->
	gen_server:cast(Pid, {put, Key, Value, Origin}).
	
get(Pid, Key, Origin) ->
	gen_server:cast(Pid, {get, Key, Origin}).

delete(Pid, Key, Origin) ->
	gen_server:cast(Pid, {delete, Key, Origin}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Partition_Number]) ->
	T_Id = ?ETS(Partition_Number),
	ets:new(T_Id, [named_table, protected]),
	lager:info("Storage for Partition ~p initialized", [Partition_Number]),
    {ok, #state{partition_number = Partition_Number, ets_name = T_Id}}.

handle_cast({get, Key, Origin}, State) ->
	#state{ets_name = Ets_Name} = State,
	case ets:lookup(Ets_Name, Key) of
		[{_, Value}] -> Reply = Value;
		[] -> Reply = notfound
	end,
	gen_server:cast({sulibarri_dht_client, Origin}, {return, Reply}),
	{noreply, State};

handle_cast({put, Key, Value, Origin}, State) ->
	#state{ets_name = Ets_Name} = State,
	ets:insert(Ets_Name, {Key, Value}),
    {noreply, State};

handle_cast({delete, Key, Origin}, State) ->
	#state{ets_name = Ets_Name} = State,
	ets:delete(Ets_Name, Key),
	{noreply, State};

handle_cast({init_transfer, Destination}, State) ->
	#state{partition_number = P_Id, ets_name = Ets_Name} = State,
	lager:info("Transfer initiated on Partition ~p, sending to ~p",
				[P_Id, Destination]),
	Data = ets:tab2list(Ets_Name),
	case Data of
		[] -> ok;
		_ ->
			gen_server:cast({sulibarri_dht_node, Destination},
							{receive_transfer, P_Id, Data, node()})
	end,
	{noreply, State};

handle_cast({receive_transfer, Data, Sender}, State) ->
	#state{partition_number = P_Id, ets_name = Ets_Name} = State,
	lager:info("Transfer Recieved on Partition ~p, from ~p", [P_Id, Sender]),
	ets:insert(Ets_Name, Data),
	{noreply, ok}.

handle_call(state, _From, State) ->
    {reply, State, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

