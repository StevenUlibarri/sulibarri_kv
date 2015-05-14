-module(sulibarri_dht_object).

-export([new_object/2,
		 get_values/1,
		 get_dots/1,
		 get_dotted_values/1,
		 set_value/3,
		 add_value/3,
		 get_clock/1,
		 increment_clock/2,
		 get_key/1,
		 get_deleted/1,
		 set_deleted/2,
		 get_dot/2,
		 merge_objects/2]).

-export_type([dht_object/0]).

-type value() :: {sulibarri_dht_vclock:dot(), term()}.

-record(object, {
	key :: term(),
	values = [] :: [value()],
	clock = [] :: sulibarri_dht_vclock:vclock(),
	deleted = false :: boolean()
	}).

-opaque dht_object() :: #object{}.

new_object(Key, Value) ->
	Obj = #object{key = Key, values = [{undefined,Value}]},
	Obj.

get_values(Obj) ->
	lists:foldl(
		fun({_, Val}, Acc) -> [Val | Acc] end,
		[],
		get_dotted_values(Obj)
	).

get_dots(Obj) ->
	dots(Obj#object.values).

get_dotted_values(Obj) ->
	Obj#object.values.

set_value(Obj, Value, Dot) ->
	Obj#object{values = [{Dot, Value}]}.

add_value(Obj, Value, Dot) ->
	#object{values = Values} = Obj,
	Obj#object{values = sort_values([{Dot, Value} | Values])}.

get_clock(Obj) ->
	Obj#object.clock.

increment_clock(Obj, VNode_Id) ->
	Obj#object{clock = 
		sulibarri_dht_vclock:increment(Obj#object.clock, VNode_Id)}.

get_key(Obj) ->
	Obj#object.key.

get_deleted(Obj) ->
	Obj#object.deleted.

set_deleted(Obj, Bool) ->
	Obj#object{deleted = Bool}.

get_dot(Obj, VNode_Id) ->
	lists:keyfind(VNode_Id, 1, Obj#object.clock).

merge_objects(Obj_Local, Obj_Incoming) ->
	New_Clock = sulibarri_dht_vclock:merge(
			sulibarri_dht_object:get_clock(Obj_Local),
			sulibarri_dht_object:get_clock(Obj_Incoming)),
	New_Values = merge_values(sulibarri_dht_object:get_values(Obj_Local),
			sulibarri_dht_object:get_values(Obj_Incoming)),
	Obj_Local#object{clock = New_Clock, values = New_Values}.

% @private
sort_values(Values) ->
	case length(Values) > 1 of
		false -> Values;
		true -> lists:sort(
			fun({_, Count1}, {_, Count2}) ->
				case Count1 =< Count2 of
					true -> true;
					false -> false
				end
			end,
				Values)
	end.

% @private
dots(Values) ->
	lists:foldl(fun({Dot, _}, Acc) -> [Dot|Acc] end,
		[],
		Values).

% @private
merge_values(Local_Vals, Incoming_Vals) ->
	{Keep, Maybe_Drop} = get_drop_candidates(Local_Vals, Incoming_Vals),
	Not_Dropped = filter_drop_candidates(Maybe_Drop),
	sort_values(Keep ++ Not_Dropped).

% @private
get_drop_candidates(Local, Incoming) -> get_drop_candidates(Local, Incoming,
													Local, Incoming, {[], []}).
get_drop_candidates([], [], _, _, {Keep, Drop}) -> {lists:ukeysort(1,Keep), Drop};
get_drop_candidates([], [H|T], LR, IR, {Keep, Drop}) ->
	{Dot, _} = H,
	case lists:keymember(Dot, 1, LR) of
		true -> get_drop_candidates([], T, LR, IR, {[H | Keep], Drop});
		false -> get_drop_candidates([], T, LR, IR, {Keep, [H|Drop]})
	end;

get_drop_candidates([H|T], Inc, LR, IR, {Keep, Drop}) ->
	{Dot, _} = H,
	case lists:keymember(Dot, 1, IR) of
		true -> get_drop_candidates(T, Inc, LR, IR, {[H|Keep], Drop});
		false -> get_drop_candidates(T, Inc, LR, IR, {Keep, [H|Drop]})
	end.

% @private
filter_drop_candidates(Values) -> 
	Dots = dots(Values),
	lists:foldl(
		fun(Val, Acc) ->
			{Dot, _} = Val,
			Rem = lists:dropwhile(
				fun(D) -> not sulibarri_dht_vclock:dominates([D], [Dot]) end,
				Dots),
			case Rem of
				[] -> [Val | Acc];
				_ -> Acc
			end
		end,
		[],
		Values
	).



