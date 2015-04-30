%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_ring_manager).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-record(state, {partition_table,
				nodes,
                n_val}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        partition_table/0,
        state/0,
        new_cluster/0,
        join_cluster/1
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

partition_table() ->
	gen_server:call(?SERVER, partition_table).

state() ->
	gen_server:call(?SERVER, state).

new_cluster() ->
	gen_server:cast(?SERVER, new_cluster).

join_cluster(Node) ->
	gen_server:cast(?SERVER, {join_cluster, Node}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
	State = inactive,
    {ok, State}.

handle_cast(new_cluster, _State) ->
	Partition_Table = sulibarri_dht_ring:brand_new_ring(),
	New_State = #state{partition_table = Partition_Table,
				    nodes = [node()],
                    n_val = 1},
    {noreply, New_State};

handle_cast({join_cluster, Node}, State) ->
    case net_adm:ping(Node) of
        pang -> {reply, {error, unreachable}, State};
        pong ->

            Cluster_State = gen_server:call({?SERVER, Node}, state),
            #state{partition_table = Table, nodes = Nodes} = Cluster_State,
            New_Table = sulibarri_dht_ring:get_new_ring(Nodes, node(), Table),
            New_Nodes = Nodes ++ [node()],
            New_N_Val = get_n_val(New_Nodes),

            New_State = #state{partition_table = New_Table,
                                nodes = New_Nodes, n_val = New_N_Val},

            lists:foreach(
                fun(N) -> gen_server:cast({?SERVER, N}, {new_state, New_State}) end,
                Nodes
            ),
            {noreply, New_State}
    end;

handle_cast({new_state, New_State}, State) ->
    lager:info("Recieved new ring state"),
    #state{partition_table = Old_Table} = State,
    #state{partition_table = New_Table} = New_State,

    Transfers = sulibarri_dht_ring:get_transfers(Old_Table, New_Table, node()),

    gen_server:cast(sulibarri_dht_node, {init_transfers, Transfers}),

    {noreply, New_State}.

handle_call(partition_table, _From, State) ->
    #state{partition_table = Table} = State,
    {reply, Table, State};

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

get_n_val(Nodes) when is_list(Nodes) ->
    get_n_val(length(Nodes));
get_n_val(N) when N =< 3 -> N;
get_n_val(N) when N > 3 -> 3.