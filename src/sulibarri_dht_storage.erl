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
        put/3,
        get/2,
        delete/2
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

put(Pid, Key, Value) ->
	gen_server:cast(Pid, {put, Key, Value}).
	
get(Pid, Key) ->
	gen_server:call(Pid, {get, Key}).

delete(Pid, Key) ->
	gen_server:cast(Pid, {delete, Key}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Partition_Number]) ->
	T_Id = ?ETS(Partition_Number),
	ets:new(T_Id, [named_table, protected]),
	lager:info("Storage for Partition ~p initialized", [Partition_Number]),
    {ok, #state{partition_number = Partition_Number, ets_name = T_Id}}.

handle_call({get, Key}, _From, State) ->
	#state{ets_name = Ets_Name} = State,
	case ets:lookup(Ets_Name, Key) of
		[{_, Value}] -> {reply, Value, State};
		[] -> {reply, not_found, State}
	end.

handle_cast({put, Key, Value}, State) ->
	#state{ets_name = Ets_Name} = State,
	ets:insert(Ets_Name, {Key, Value}),
    {noreply, State};

handle_cast({delete, Key}, State) ->
	#state{ets_name = Ets_Name} = State,
	ets:delete(Ets_Name, Key),
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

