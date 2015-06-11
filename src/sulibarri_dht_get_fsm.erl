-module(sulibarri_dht_get_fsm).

-export([start_link/2, create/2]).

-behaviour(gen_fsm).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3, handle_sync_event/4]).

-define(SERVER, ?MODULE).

-record(state,{
		origin,
		key,
		n, r,
		pref_list = [],
		acks = 0,
		responses = []
	}).

start_link(Key, Origin) ->
	gen_fsm:start_link(?MODULE, [Key, Origin], []).

create(Key, Origin) ->
	sulibarri_dht_get_fsm_sup:start_child(Key, Origin).

%% @private
init([Key, Origin]) ->
	State = #state{origin = Origin,
				   key = Key},
	{ok, prepare, State, 0}.

%% @private
prepare(timeout, State = #state{key = Key,
								origin = Origin}) ->

	Ring = sulibarri_dht_ring_manager:get_ring_state(),
	Table = sulibarri_dht_ring:get_partition_table(Ring),
	{N, _, R} = sulibarri_dht_ring:get_replication_factors(Table),
	Pref_List = sulibarri_dht_ring:get_subbed_pref_list(Key, Ring),

	New_State = State#state{n = N, r = R, pref_list = Pref_List},

	Up_Primaries = sulibarri_dht_ring:filterd_primaries(Key, Ring),

	LocalPrimary = [Coord || {Node, _} = Coord <- Up_Primaries, Node =:= node()],

	case {Up_Primaries, LocalPrimary =:= []} of
		{[], _} ->
			{next_state, validate, New_State, 0};
		{_, true} ->
			Random_Primary = lists:nth(random:uniform(length(Up_Primaries)), Up_Primaries),
			supervisor:start_child({sulibarri_dht_get_fsm, Random_Primary}, [Key, Origin]),
			{stop, normal, New_State};
		_ ->
			{next_state, validate, New_State, 0}
	end.

validate(timeout, State = #state{r = R,
								 pref_list = Pref_List,
								 origin = Origin,
								 key = Key}) ->
	case length(Pref_List) of
		0 ->
			Error = sulibarri_dht_error:make_error(put, Key, all_nodes_down),
			sulibarri_dht_client:reply(Origin, Error);
		UpNodes when UpNodes < R ->
			Error = sulibarri_dht_error:make_error(get, Key,
													{r_val_violation,
													{required, R},
													{actual, UpNodes}}),
			sulibarri_dht_client:reply(Origin, Error);
		_ ->
			{next_state, execute, State, 0}
	end.

execute(timeout, State = #state{pref_list = Pref_List, key = Key}) ->
	lists:foreach(
		fun({Node, VNodeId}) ->
			sulibarri_dht_vnode_router:route_get(Node, VNodeId, {get, Key, self()})
		end,
		Pref_List
	),
	{next_state, waiting, State}.

waiting(Resp, State =#state{origin = Origin,
												 key = Key,
												 acks = Acks,
												 pref_list = Pref_List,
												 r = R, n = N,
												 responses = Responses}) ->
	Inc_Acks = Acks + 1,
	New_Responses = [Resp | Responses],
	New_State = State#state{acks = Inc_Acks, responses = New_Responses},
	case Inc_Acks of
		R ->
			Merged_Obj = merge_responses(New_Responses),
			case Merged_Obj of
				not_found ->
					sulibarri_dht_client:reply(Origin, {info, {not_found, Key}});
				_ ->
					sulibarri_dht_client:reply(Origin, {info, {ok, Merged_Obj}})
			end,
			{next_state, waiting, New_State};		
		N ->
			Merged_Obj = merge_responses(New_Responses),
			case Merged_Obj of
				not_found -> ok;
				_ ->
					lists:foreach(
						fun({Node, VNodeId}) ->
							sulibarri_dht_vnode_router:route(Node, VNodeId, {replicate_put, Merged_Obj, undefined})
						end,
						Pref_List
					)
			end,
			{stop, normal, New_State};
		_ ->
			New_State = State#state{acks = Inc_Acks,
									responses =[Resp | Responses]},
			{next_state, waiting, New_State}
	end.

% merge_responses(Responses) ->
% 	{Objs, _} = lists:unzip(Responses),
% 	merge_obj(Objs).

merge_responses(Objs) when length(Objs) =:= 1 ->
	hd(Objs);
merge_responses(Objs) ->
	Merged_Obj = lists:foldl(
		fun(Obj, M_Obj) ->
			case {Obj, M_Obj} of
				{not_found, _} -> M_Obj;
				{Obj, undefined} -> Obj;
				{Obj, M_Obj} ->
					sulibarri_dht_object:merge_objects(Obj, M_Obj)
			end
		end,
		undefined,
		Objs
	),
	case Merged_Obj of
		undefined -> not_found;
		_ -> Merged_Obj
	end.

%% @private
initial_state(_Event, _From, State) ->
	{reply, ok, initial_state, State}.

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
terminate(_Reason, _StateName, _State) ->
	ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
	{ok, StateName, State}.