-module(sulibarri_dht_put_fsm).


-export([start_link/2]).

-behaviour(gen_fsm).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3, handle_sync_event/4]).

-compile([export_all]).


-define(SERVER, ?MODULE).

-record(state, {
		origin,
		inc_object,
		n, w,
		pref_list = [],
		frontier_obj,
		acks = 0
	}).

start_link(Obj, Origin) ->
	gen_fsm:start_link(?MODULE, [Obj, Origin], []).

create(Obj, Origin) ->
	sulibarri_dht_put_fsm_sup:start_child(Obj, Origin).

%% @private
init([Obj, Origin]) ->
	State = #state{origin = Origin,
				   inc_object = Obj
				   },
	{ok, prepare, State, 0}.

%% @private
prepare(timeout, State = #state{inc_object = Obj,
							  origin = Origin}) ->

	Ring = sulibarri_dht_ring_manager:get_ring_state(),
	Table = sulibarri_dht_ring:get_partition_table(Ring),
	{N, W, _} = sulibarri_dht_ring:get_replication_factors(Table),
	Obj_Key = sulibarri_dht_object:get_key(Obj),
	Pref_List = sulibarri_dht_ring:get_subbed_pref_list(Obj_Key, Ring),
	New_State = State#state{n = N, w = W,
							pref_list = Pref_List},

	Up_Primaries = sulibarri_dht_ring:filtered_primaries(Obj_Key,Ring),

	%% am I a primary?
	LocalPrimary = [Coord || {Node, _} = Coord <- Up_Primaries, Node =:= node()],

	case {Up_Primaries, LocalPrimary =:= []} of
		{[], _} ->
			%all primaries down Coord From here
			{next_state, validate, New_State, 0};
		{_, true} ->
			%I am not a primary, forward to random primary
			Random_Primary = lists:nth(random:uniform(length(Up_Primaries)), Up_Primaries),
			supervisor:start_child({sulibarri_dht_put_fsm, Random_Primary}, [Obj, Origin]),
			{stop, normal, New_State};
		_ ->
			%I am a primary, I can coord
			{next_state, validate, New_State, 0}
	end.

validate(timeout, State = #state{w = W,
								 pref_list = Pref_List,
								 origin = Origin,
								 inc_object = Obj}) ->
	%check for W violations
	case length(Pref_List) of
		0 -> 
			Error = sulibarri_dht_error:make_error(put, Obj, all_nodes_down),
			sulibarri_dht_client:reply(Origin, Error);
		UpNodes when UpNodes < W -> 
			Error = sulibarri_dht_error:make_error(put, Obj,
													{w_val_violation,
													{required, W},
													{actual, UpNodes}}),
			sulibarri_dht_client:reply(Origin, Error);
		_ -> 
			{next_state, execute_local, State, 0}
	end.
	
execute_local(timeout, State = #state{pref_list = Pref_List,
									  inc_object = Obj}) ->
	{Node, Id} = lists:keyfind(node(), 1, Pref_List),
	% {{Node, Id, _}, _} = hd(Pref_List), %%%% CHECK HANDOFF %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	sulibarri_dht_vnode_router:route(Node, Id, {local_put, Obj, self()}),
	{next_state, waiting_local, State}.

%% @private
waiting_local(Result, State = #state{origin  = Origin, acks = Acks, w = W}) ->
	% lager:info("~p", [lager:pr(Result, sulibarri_dht_object)]),
	New_State = State#state{frontier_obj = Result, acks = Acks + 1},

	case New_State#state.acks of 
		W -> 
			sulibarri_dht_client:reply(Origin, {info, put_success}),
			{stop, normal, New_State};
		_ -> {next_state, execute_remote, New_State, 0}
	end.

execute_remote(timeout, State = #state{pref_list = Pref_List,
									   frontier_obj = Frontier_Obj}) ->
Remotes = lists:keydelete(node(), 1, Pref_List),

lists:foreach(
	fun({Node, VNode_Id}) ->
		sulibarri_dht_vnode_router:route(Node, VNode_Id, {replicate_put, Frontier_Obj, self()})
	end,
	Remotes
),
{next_state, waiting_remote, State}.

waiting_remote(_Result, State = #state{origin = Origin, acks = Acks, w = W}) ->
New_State = State#state{acks = Acks + 1},
case New_State#state.acks of 
		W -> 
			sulibarri_dht_client:reply(Origin, {info, put_success}),
			{stop, normal, New_State};
		_ -> {next_state, waiting_remote, New_State}
end.

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


