-module(sulibarri_dht_put_fsm).


-export([start_link/2]).

-behaviour(gen_fsm).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3, handle_sync_event/4]).

-compile([export_all]).


-define(SERVER, ?MODULE).

-record(state, {
		origin,
		inc_object,
		n, w, r,
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
	{N, W, R} = sulibarri_dht_ring:get_replication_factors(Table),
	Obj_Key = sulibarri_dht_object:get_key(Obj),
	Pref_List = sulibarri_dht_ring:get_pref_list(Obj_Key, Ring),
	io:format("~p~n", [Pref_List]),
	New_State = State#state{n = N, w = W, r = R,
							pref_list = Pref_List},

	LocalPrimary = [Coord || {{Node,_, _}, Role} = Coord <- Pref_List, Node =:= node(), Role =:= primary],

	case {Pref_List, LocalPrimary =:= []} of
		{[], _} ->
			% all primaries down coord from here
			{next_state, validate, New_State, 0};
		{_, true} ->
			% node is not a primary, forward to random primary
			Primaries = sulibarri_dht_ring:primaries(Pref_List),
			Random_Primary = lists:nth(random:uniform(length(Primaries)), Primaries),
			supervisor:start_child({sulibarri_dht_put_fsm_sup, Random_Primary},
									[Obj, Origin]),
			{stop, normal, New_State};
		_ ->
			%node is primary, can coord
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
	{{Node, Id, _}, _} = hd(Pref_List), %%%% CHECK HANDOFF %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

% execute_remote(timeout, State = #state{n = N, w = W,
% 									   pref_list = Pref_List,
% 									   frontier_obj = Frontier_Obj}) ->
% Targets0 = lists:sublist(N, Pref_List),
% Targets1 - lists:filter(fun({{Node, _}, _}) -> Node =/= node() end, Targets0),

% lists:foreach(
% 	fun({{Node, VNode_Id}, }) %% MAP FALLBACKS TO THE PRIMARY %%
% )


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


