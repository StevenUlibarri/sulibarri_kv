-module(sulibarri_dht_vnode).

-export([start_link/1]).

-behaviour(gen_fsm).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3, handle_sync_event/4]).

-define(FILE_PATH(VnodeID),
    "storage/" ++ atom_to_list(node()) ++ "/" ++ integer_to_list(VNodeId) ++ ".store").
% -define(HINTED_FILE_PATH(VnodeID),
%     "storage/" ++ atom_to_list(node()) ++ "/hints_" ++ integer_to_list(VNodeId) ++ ".store").
-define(DETS_ARGS, [{keypos, 2}]).

-include("dht_object.hrl").
-include("vnode_ops.hrl").

-record(state, {vNodeId,
                storage_file_path,
                % hinted_file_path,
                forward_node}).

% create(VNodeId) ->
%     sulibarri_dht_vnode_sup:start_child(VNodeId).

start_link(VNodeId) ->
    gen_fsm:start_link(?MODULE, [VNodeId], []).

%% @private
init([VNodeId]) ->
    % sulibarri_dht_vnode_router:register(VNodeId, self()),
    State = #state{vNodeId = VNodeId,
                    storage_file_path = ?FILE_PATH(VNodeId)},
                    % hinted_file_path = ?HINTED_FILE_PATH(VNodeId)},
    % lager:info("Vnode ~p active", [VNodeId]),
    {ok, active, State}.

%% Active
active({local_put, Obj_Inc, Fsm_Sender}, State) ->
    Res = case get(Obj_Inc#object.key, State#state.storage_file_path) of
        {error, _} = Err -> Err;
        [] -> 
            New_Obj = clock_ops(Obj_Inc, State#state.vNodeId),
            case ?MODULE:put(New_Obj, State#state.storage_file_path) of
                ok -> New_Obj;
                Err -> Err
            end;
        [Obj_Local] ->
            New_Obj = clock_ops(Obj_Inc, Obj_Local, State#state.vNodeId),
            case ?MODULE:put(New_Obj, State#state.storage_file_path) of
                ok -> New_Obj;
                Err -> Err
            end
    end,
    % lager:info("~p", [lager:pr(Res, sulibarri_dht_object)]),
    reply(Fsm_Sender, Res),
    {next_state, active, State};
   
active({replicate_put, Obj_Inc, Fsm_Sender}, State) ->
    Res = case get(Obj_Inc#object.key, State#state.storage_file_path) of
        {error, _} = Err -> Err;
        [] ->
            case ?MODULE:put(Obj_Inc, State#state.storage_file_path) of
                ok -> Obj_Inc;
                Err -> Err
            end;
        Obj_Local ->
            New_Obj = replica_merge(Obj_Inc, Obj_Local),
            case ?MODULE:put(New_Obj, State#state.storage_file_path) of
                ok -> Obj_Inc;
                Err -> Err
            end
    end,
    reply(Fsm_Sender, Res),
    {next_state, active, State};


% active({local_delete, Obj, Fsm_Sender}, State) ->
%     ;
% active({replicate_delete, Obj, Fsm_Sender}, State) ->
%     ;

active({get, Key, Fsm_Sender}, State) ->
    Res = case get(Key, State#state.storage_file_path) of
        {error, _} = Err -> Err;
        [] -> not_found;
        Obj -> Obj
    end,
    reply(Fsm_Sender, Res),
    {next_state, active, State};

% active({read_repair, Obj, Fsm_Sender}, State) ->
%     ;
% active({init_handoff, Destination}, State)->
%     ok.

active(stop, State) ->
    {stop, normal, State}.


% active({replicate_put, Obj, Fsm_Sender}, State) ->
%     ;
% active({local_delete, Obj, Fsm_Sender}, State) ->
%     ;
% active({replicate_delete, Obj, Fsm_Sender}, State) ->
%     ;
% active({get, Key, Fsm_Sender}, State) ->
%     ;
% active({read_repair, Obj, Fsm_Sender}, State) ->
%     ;
% active({init_handoff, Destination}, State)->
%     ok.


%% @private
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

%% @private
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @private
terminate(_Reason, _StateName, State) ->
    sulibarri_dht_vnode_router:degregister(State#state.vNodeId),
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% backend stuff

get(Key, File) ->
    dets:open_file(File, ?DETS_ARGS),
    Obj = dets:lookup(File, Key),
    dets:close(File),
    Obj.

put(Obj, File) ->
    dets:open_file(File, ?DETS_ARGS),
    Res = dets:insert(File, Obj),
    dets:close(File),
    Res.

% delete(Obj, File) ->
%     % dets:open_file(File, []),
%     ok.

reply(Fsm_Sender, Message) ->
    gen_fsm:send_event(Fsm_Sender, Message).

clock_ops(Inc, Local, Id) ->
    Clock_Inc = sulibarri_dht_object:get_clock(Inc),
    Clock_Local = sulibarri_dht_object:get_clock(Local),

    case sulibarri_dht_vclock:descends(Clock_Inc, Clock_Local) of
        true ->
            New_Obj1 = sulibarri_dht_object:increment_clock(Inc, Id),
            [Val] = sulibarri_dht_object:get_values(New_Obj1),
            Dot = sulibarri_dht_object:get_dot(New_Obj1, Id),
            New_Obj2 = sulibarri_dht_object:set_value(New_Obj1, Val, Dot),
            New_Obj2;
        false ->
            Merged_Clock = sulibarri_dht_vclock:merge(Clock_Inc, Clock_Local),
            Incremented_Clock = sulibarri_dht_vclock:increment(Merged_Clock, Id),
            [New_Val] = sulibarri_dht_object:get_values(Inc),
            Dot = sulibarri_dht_object:get_dot(Incremented_Clock, Id),
            Dotted_Inc_Value = {Dot, New_Val},

            New_Obj1 = Local#object{clock = Incremented_Clock},
            New_Obj2 = sulibarri_dht_object:add_value(New_Obj1, Dotted_Inc_Value),
            New_Obj2
    end.

%% @private
clock_ops(Inc, Id) ->
    New_Obj1 = sulibarri_dht_object:increment_clock(Inc, Id),
    [Val] = sulibarri_dht_object:get_values(New_Obj1),
    Dot = sulibarri_dht_object:get_dot(New_Obj1, Id),
    New_Obj2 = sulibarri_dht_object:set_value(New_Obj1, Val, Dot),
    New_Obj2.

replica_merge(Inc, Local) ->
    #object{clock = Clock_Inc} = Inc,
    #object{clock = Clock_Local} = Local,

    case sulibarri_dht_vclock:dominates(Clock_Inc, Clock_Local) of
        true -> Inc;
        false ->
            New_Obj = sulibarri_dht_object:merge_objects(Inc, Local),
            New_Obj
    end. 





