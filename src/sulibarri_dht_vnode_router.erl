%%%-------------------------------------------
%%% @author 
%%% @copyright
%%% @doc 
%%% @end
%%%-------------------------------------------

-module(sulibarri_dht_vnode_router).
-behaviour(gen_server).

-define(ROUTE_TABLE, local).
-define(HINTED_TABLE, hinted).
-define(HINTED_TABLE_PATH, "storage/" ++ atom_to_list(node()) ++ "/hints_table.hints").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
        start_link/0,
        route/3,
        start_vnode/1,
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

route(Node, VNodeId, Op) ->
    gen_server:cast({?MODULE, Node}, {route, VNodeId, Op}).

start_vnode(VNodeIds) ->
    gen_server:cast(?MODULE, {start_vnode, VNodeIds}).

% register(VNodeId, Pid) ->
%     gen_server:cast(?MODULE, {register, VNodeId, Pid}).

degregister(VNodeId) ->
    gen_server:cast(?MODULE, {degregister, VNodeId}).

get_routes() ->
    gen_server:call(?MODULE, get_routes).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    ets:new(?ROUTE_TABLE, [named_table]),
    ets:new(?HINTED_TABLE, [named_table]),
    dets:open_file(?HINTED_TABLE_PATH, []),
    ets:from_dets(?HINTED_TABLE, ?HINTED_TABLE_PATH),

    Hinted_Entries = ets:tab2list(?HINTED_TABLE),
    ets:delete_all_objects(?HINTED_TABLE),
    Hinted_Ids = lists:foldl(
        fun({Id, _}, Acc) ->
            [Id | Acc]
        end,
        [],
        Hinted_Entries
    ),
    dets:close(?HINTED_TABLE_PATH),
    start_vnodes(lists:reverse(Hinted_Ids), ?HINTED_TABLE),
    {ok, Args}.

handle_call(get_routes, _From, State) ->
    List = ets:tab2list(?ROUTE_TABLE),
    {reply, List, State}.

handle_cast({route, VNodeId, Op}, State) ->
    Ring_State = sulibarri_dht_ring_manager:get_ring_state(),
    case sulibarri_dht_ring:vnode_belongs_to(node(), VNodeId, Ring_State) of
        true ->
            [{_,Pid}] = ets:lookup(?ROUTE_TABLE, VNodeId),
            gen_fsm:send_event(Pid, Op);
        false ->
            case ets:lookup(?HINTED_TABLE, VNodeId) of
                [] ->
                    start_vnodes([VNodeId], ?HINTED_TABLE),
                    [{_, Pid}] = ets:lookup(?HINTED_TABLE, VNodeId),
                    gen_fsm:send_event(Pid, Op);
                [{_, Pid}] ->
                    gen_fsm:send_event(Pid, Op)
            end
    end,
    {noreply, State};

handle_cast({start_vnode, VNode_Ids}, State) ->
    start_vnodes(VNode_Ids, ?ROUTE_TABLE),
    {noreply, State};

% handle_cast({register, VNodeId, Pid}, State) ->
%     register_vnode(VNodeId,)
%     {noreply, State};

handle_cast({degregister, VNodeId}, State) ->
    Ring_State = sulibarri_dht_ring_manager:get_ring_state(),
    case sulibarri_dht_ring:vnode_belongs_to(node(), VNodeId, Ring_State) of
        true -> deregister_vnode(VNodeId, ?ROUTE_TABLE);
        false -> 
            deregister_vnode(VNodeId, ?HINTED_TABLE),
            maybe_persist_handoffs(?HINTED_TABLE)
    end,
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

start_vnodes(VNode_Ids, Table) ->
    lists:foreach(
        fun(Id) ->
            {ok, Pid} = sulibarri_dht_vnode_sup:start_child(Id),
            register_vnode(Id, Pid, Table)
        end,
        VNode_Ids
    ),
    maybe_persist_handoffs(Table).

register_vnode(Id, Pid, Table) ->
    ets:insert(Table, {Id, Pid}),
    lager:info("~p vnode ~p registered", [Table, Id]).

deregister_vnode(Id, Table) ->
    ets:delete(Table, Id),
    lager:info("~p vnode ~p degregistered", [Table, Id]).

maybe_persist_handoffs(Table) ->
    case Table of
        ?HINTED_TABLE ->
            dets:open_file(?HINTED_TABLE_PATH, []),
            ets:to_dets(?HINTED_TABLE, ?HINTED_TABLE_PATH),
            dets:close(?HINTED_TABLE_PATH);
        _ -> ok
    end.