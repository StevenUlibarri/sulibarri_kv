-module(sulibarri_dht_client).

%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        new_cluster/1,
        join_cluster/1,
        put/2,
        get/1,
        delete/1
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

new_cluster(Nodes) ->
	gen_server:cast(?SERVER, {new_cluster, Nodes}).

join_cluster(Node) ->
	gen_server:cast(?SERVER, {join_cluster, Node}).

put(Key, Value) ->
	gen_server:cast(?SERVER, {put, Key, Value}).

get(Key) ->
	gen_server:cast(?SERVER, {get, Key}).

delete(Key) ->
	gen_server:cast(?SERVER, {delete, Key}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({new_cluster, Nodes}, State) ->
	sulibarri_dht_node:new_cluster(Nodes),
    {noreply, State};

handle_cast({join_cluster, Node}, State) ->
	sulibarri_dht_node:join_cluster(Node),
	{noreply, State};

handle_cast({put, Key, Value}, State) ->
	sulibarri_dht_node:put(Key, Value, node()),
	{noreply, State};

handle_cast({get, Key}, State) ->
	sulibarri_dht_node:get(Key, node()),
	{noreply, State};

handle_cast({delete, Key}, State) ->
	sulibarri_dht_node:delete(Key, node()),
	{noreply, State};

handle_cast({return, Data}, State) ->
	io:format("Recieved ~p~n", [Data]),
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






