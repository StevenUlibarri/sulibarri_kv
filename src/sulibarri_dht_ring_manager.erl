%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_ring_manager).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-record(state, {active,
				partition_table,
				nodes}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        active/0,
        partition_table/0,
        state/0,
        cluster/0,
        cluster/1
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

active() ->
	gen_server:call(?SERVER, {active}).

partition_table() ->
	gen_server:call(?SERVER, {partition_table}).

state() ->
	gen_server:call(?SERVER, {state}).

cluster() ->
	gen_server:call(?SERVER, {cluster}).

cluster(Node) ->
	gen_server:cast(?SERVER, {cluster, Node}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
	State = #state{active = false},
    {ok, State}.

handle_call({cluster}, _From, _State) ->
	Partition_Table = sulibarri_dht_hash:new_ring(node()),
	Partition_List = orddict:fetch_keys(Partition_Table),
	State = #state{active = true,
					partition_table = Partition_Table,
					nodes = [node()]},
    {reply, Partition_List, State};

handle_call({active}, _From, State) ->
    #state{active = Active} = State,
    {reply, Active, State};

handle_call({partition_table}, _From, State) ->
    #state{partition_table = Table} = State,
    {reply, Table, State};

handle_cast({cluster, Node}, State) ->
    .

handle_cast(_Msg, State) ->
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

