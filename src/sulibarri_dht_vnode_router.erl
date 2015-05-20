%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_vnode_router).
-behaviour(gen_server).

-define(ROUTE_TABLE, routes).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        route/2,
        register/2,
        degregister/1,
        get_routes/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

route(VNodeId, Op) ->
    gen_server:cast(?MODULE, {route, VNodeId, Op}).

register(VNodeId, Pid) ->
    gen_server:cast(?MODULE, {register, VNodeId, Pid}).

degregister(VNodeId) ->
    gen_server:cast(?MODULE, {degregister, VNodeId}).

get_routes() ->
    gen_server:call(?MODULE, get_routes).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ets:new(?ROUTE_TABLE, [named_table]),
    {ok, Args}.

handle_call(get_routes, _From, State) ->
    List = ets:tab2list(?ROUTE_TABLE),
    {reply, List, State}.

handle_cast({route, VNodeId, Op}, State) ->
    Pid = ets:lookup(?ROUTE_TABLE, VNodeId),
    gen_fsm:send_event(Pid, Op),
    {noreply, State};

handle_cast({register, VNodeId, Pid}, State) ->
    ets:insert(?ROUTE_TABLE, {VNodeId, Pid}),
    {noreply, State};

handle_cast({degregister, VNodeId}, State) ->
    ets:delete(?ROUTE_TABLE, VNodeId),
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

