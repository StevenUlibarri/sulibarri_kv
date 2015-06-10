-module(sulibarri_dht_vclock).

-export([increment/2,
		 descends/2,
		 dominates/2,
		 merge/2,
		 equal/2]).

% -type dot() :: {term(), pos_integer()}.
% -type vclock() :: [dot()].

% -export_type([dot/0, vclock/0]).

increment([], VNode_Id) ->
	[{VNode_Id, 1}];
increment(Clock, VNode_Id) ->
	case lists:keyfind(VNode_Id, 1, Clock) of
		false -> [{VNode_Id, 1} | Clock];
		{_ , Num} -> NewClock = lists:keyreplace(VNode_Id, 1, Clock,
								{VNode_Id, Num + 1}),
		sort(NewClock)
	end.

descends(_, []) -> true;
descends(Clock1, Clock2) ->
	[{Node2, Count2} | Rest2] = Clock2,
	case lists:keyfind(Node2, 1, Clock1) of
		false -> false;
		{_, Count1} ->
			Count1 >= Count2 andalso
			descends(Clock1, Rest2)
	end.

equal(Clock1, Clock2) ->
	descends(Clock1, Clock2) andalso descends(Clock2, Clock1).

dominates(Clock1, Clock2) ->
	descends(Clock1, Clock2) andalso not descends(Clock2, Clock1).

merge([], Clock) -> Clock;
merge(Clock, []) -> Clock;
merge(Clock1, Clock2) -> merge(Clock1, Clock2, []).

merge([], Clock2, Acc) -> sort(Clock2 ++ Acc);
merge(Clock1, [], Acc) -> sort(Clock1 ++ Acc);
merge([], [], Acc) -> sort(Acc);
merge(Clock1, Clock2, Acc) ->
	[{Node1, Count1} | Clock1Rest] = Clock1,
	case lists:keytake(Node1, 1, Clock2) of
		{value, {_, Count2}, Clock2Rest} ->
			merge(Clock1Rest, Clock2Rest, [{Node1, greater_of(Count1, Count2)} | Acc]);
		false ->
			merge(Clock1Rest, Clock2, [{Node1, Count1} | Acc])
	end.

% @private
sort(Clock) ->
	case length(Clock) > 1 of
		false -> Clock;
		true -> lists:keysort(1, Clock)
	end.

% @private
greater_of(Num1, Num2) ->
	case Num1 > Num2 of
		true -> Num1;
		false -> Num2
	end.