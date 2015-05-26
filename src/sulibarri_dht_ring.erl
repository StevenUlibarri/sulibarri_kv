-module(sulibarri_dht_ring).
% -compile([export_all,     
% debug_info]).

-define(MAX_INDEX, (math:pow(2,160)-1)).
-define(DEFAULT_PARTITIONS, 64).

-compile([export_all]).

-export([]).



balance_ring(Nodes, Partition_Table) ->
	New_Node = {lists:last(Nodes), 0},
	Distribution = get_distribution(Partition_Table),
	Num = ?DEFAULT_PARTITIONS / length(Nodes),
	Upper = sulibarri_dht_utils:ceiling(Num),
	Lower = sulibarri_dht_utils:floor(Num),

	balance(Partition_Table, Distribution, New_Node, Upper, Lower).

balance(Partition_Table, Distribution, New_Node, Upper, Lower) ->
	balance(1, Partition_Table, Distribution, New_Node, Upper, Lower, false).

balance(Current_Index, Table, Distribution, New_Node, Upper, Lower, false) ->

	{Current_Node_Name, Hash} = lists:nth(Current_Index, Table),
	{New_Node_Name, New_Node_Count} = New_Node,

	Highest = get_highest_dist([New_Node | Distribution]),

	Can_Take = can_take(Current_Node_Name, Distribution, Lower, Highest),
	Can_Fit = can_fit(New_Node_Name, Current_Index, Table), %% CHECK

	case (Can_Take and Can_Fit) of
		true ->
			Table2 = lists:keyreplace(Current_Index, 1, Table,
											 {Current_Index, {New_Node_Name, Hash}}),
			{_, DistCount} = lists:keyfind(Current_Node_Name, 1, Distribution),
			Distribution2 = lists:keyreplace(Current_Node_Name, 1, Distribution, 
											{Current_Node_Name, DistCount - 1}),
			New_Node2 = {New_Node_Name, New_Node_Count + 1},
			Balanced = balanced([New_Node2 | Distribution2], Lower, Upper),
			balance(Current_Index+1, Table2, Distribution2, New_Node2, Upper, Lower, Balanced);
		false ->
			balance(Current_Index+1, Table, Distribution, New_Node, Upper, Lower, false)
	end;
		
balance(_, Table, _, _, _, _, true) ->
	Table. 

get_highest_dist(Distribution) ->
	lists:foldl(
		fun({_, Count}, Highest) ->
			case Count of
				N when N > Highest -> N;
				_ -> Highest
			end
		end,
		-1,
		Distribution
	).

can_take(Node, Distribution, Lower, Highest) ->
	{_, Count} = lists:keyfind(Node, 1, Distribution),
	(Count > Lower) and (Count =:= Highest).

balanced(Distribution, Lower, Upper) ->
	case lists:dropwhile(
		fun({_, Count}) ->
			(Count >= Lower) and (Count =< Upper) end,
			Distribution
	) of
		[] -> true;
		_ -> false
	end.

can_fit(Node, Partition_Id, Partition_Table) ->
	Neighbors = get_neighbors(Partition_Id, Partition_Table), %% CHECK
	Neighbor_Nodes = [N || {_, {N, _}} <- Neighbors],
	case lists:dropwhile(fun(Neighbor_Node) -> Neighbor_Node =/= Node end, Neighbor_Nodes) of
		[] -> true;
		_ -> false
	end.

get_transfers(Old_Table, New_Table, Node) ->
	Pairs = lists:zip(Old_Table, New_Table),
	Transfers = lists:foldl(
		fun(Pair, Acc) ->
			{{P_Id, {From_Node, _}},{_, {To_Node, _}}} = Pair,
			case (From_Node =/= To_Node) and (From_Node =:= Node) of
				true ->
					[{P_Id, From_Node, To_Node} | Acc];
				false ->
					Acc
			end
		end,
		[],
		Pairs
	),
	Transfers.

%% PUBLIC %%

hash(Key) -> %% MAYBE NOT PUBLIC? %%
	ByteHash = crypto:sha(term_to_binary(Key)),
	Hash = crypto:bytes_to_integer(ByteHash),
	Hash.

% add_node

% remove_node

new_ring(Node_List) -> 
	Nodes = lists:sort(Node_List),
	Table = lists:reverse(new_ring(lists:seq(1, ?DEFAULT_PARTITIONS), Nodes, Nodes, [])),
	case length(Nodes) of
		N when N =< 4 -> Table;
		N ->
			case ?DEFAULT_PARTITIONS rem N of
				Rem when (Rem =:= 0) or (Rem >= 3) -> Table;
				Rem ->
					Wrap_Indexes = lists:seq(?DEFAULT_PARTITIONS - (Rem -1), ?DEFAULT_PARTITIONS),
					check_replace(Wrap_Indexes, Table)
			end
		end.

new_ring([Current_Index|Rest_Idx], [Current_Node|Rest_Nodes], Nodes, Acc) ->
	Entry = {Current_Node, (?MAX_INDEX / ?DEFAULT_PARTITIONS) * Current_Index},
	new_ring(Rest_Idx, Rest_Nodes, Nodes, [Entry|Acc]);
new_ring([], _, _, Acc) -> Acc;
new_ring(Idxs, [], Nodes, Acc) -> new_ring(Idxs, Nodes, Nodes, Acc).

get_pref_list(Key, Table) ->
	{_, Primary_Id} = lookup(Key, Table),
	{H, T} = lists:splitwith(fun({_, Id}) -> Id =/= Primary_Id end, Table),
	Wrapped_List = T ++ H,
	{Remaining_Vnodes, Primaries} = get_primaries(Wrapped_List),
	Secondaries = get_secondaries(Remaining_Vnodes),
	{Primaries, Secondaries}.

get_vnodes_for_node(Node, Table) ->
	VNodes = lists:foldl(
		fun(Entry = {N, _}, Acc) ->
			case N =:= Node of
				true -> [Entry | Acc];
				false -> Acc
			end
		end,
		[],
		Table
	),
	lists:reverse(VNodes).

%% PRIVATE %%

check_replace(Indexes, Table) ->
	lists:foldl(
		fun(Idx, Current_Table) ->
			% get entry at idx,
			Entry = lists:nth(Idx, Table),
			% get neighbors for entry,
			Neighbors = get_neighbors(Idx, Table),
			% check if it fits,
			case can_fit(Entry, Neighbors) of
				% if not get suitable replacement from Distribution
				true -> Current_Table;
				false -> 
					Distribution = get_distribution(Current_Table),
					{Node, _} = simple_replace(Distribution, Neighbors),
					{_, VNodeId} = Entry,
					lists:sublist(Current_Table, Idx-1) ++
							[{Node, VNodeId}] ++ lists:nthtail(Idx, Current_Table)
			end
		end,
		Table,
		Indexes
	).

simple_replace([H|T], Neighbors) ->
	case can_fit(H, Neighbors) of
		true -> H;
		false -> simple_replace(T, Neighbors)
	end.


get_neighbors(Idx, Table) ->
	Window1 = lists:seq(Idx - 2, Idx + 2),
	Window2 = lists:delete(Idx, Window1),
	Window3 = check_wrap(Window2),

	lists:reverse(
		lists:foldl(
			fun(Window_Index, Acc) -> 
				[lists:nth(Window_Index, Table) | Acc]
			end,
			[],
			Window3
		)
	).

can_fit(Entry, Neighbors) ->
	{Node, _} = Entry,
	not lists:keymember(Node, 1, Neighbors).

get_distribution(Table) ->
	Dist = lists:foldl(
		fun(Entry, Acc) ->
			{Node, _} = Entry,
			case lists:keyfind(Node, 1, Acc) of
				false -> [{Node, 1} | Acc];
				{_, Count} -> lists:keyreplace(Node, 1, Acc, {Node, Count + 1})
			end
		end,
		[],
		Table),
	lists:keysort(1, Dist).

get_primaries(Table) -> get_primaries(Table, get_n_val(Table), []).
get_primaries(Table, N_Val, Primaries) ->
	case length(Primaries) of
		N_Val -> {Table, lists:reverse(Primaries)};
		_ ->
			Next_Primary = {P_Owner, _} = lists:nth(1, Table),
			New_Table = lists:filter(fun({Node, _}) -> Node =/= P_Owner end,Table),
			get_primaries(New_Table, N_Val, [Next_Primary | Primaries])
	end.

get_secondaries(Table) -> get_secondaries(Table, []).
get_secondaries([], Secondaries) -> lists:reverse(Secondaries);
get_secondaries(Table, Secondaries)->
	Next_Secondary = {S_Owner, _} = lists:nth(1, Table),
	New_Table = lists:filter(fun({Node, _}) -> Node =/= S_Owner end, Table),
	get_secondaries(New_Table, [Next_Secondary | Secondaries]).
	
lookup(Key, Table) ->
	Hash = hash(Key),
	% Hash = Key,
	lookup_node(Hash, Table).
lookup_node(Hash, [H|T]) ->
	{_, Top} = H,
	case Top >= Hash of
		true -> H;
		false -> lookup_node(Hash, T)
	end.

check_wrap(Window) ->
	Checked = lists:foldl(
		fun(P_Id, Acc) ->
			case P_Id of
				N when N > ?DEFAULT_PARTITIONS ->
					[N - ?DEFAULT_PARTITIONS | Acc];
				N when N =< 0 ->
					[N + ?DEFAULT_PARTITIONS | Acc];
				N -> [N | Acc]
			end
		end,
		[],
		Window
	),
	lists:reverse(Checked).

get_n_val(Table) ->
	Dist = get_distribution(Table),
	case length(Dist) of
		1 -> 1;
		2 -> 1;
		3 -> 2;
		_ -> 3
	end.

% get_pref_list(Partition_Id, Table, N_Val) ->
% 	{Owner, _} = lists:keyfind(Partition_Id, 1, Table),
% 	{H, T} = lists:splitwith(fun({N, _}) -> N =/= Partition_Id end, Table),
% 	WrappedTable = T ++ H,
% 	Nodes = lists:reverse(lists:foldl (
% 			fun({_, {Node, _}}, Acc) ->
% 				case (Node =/= Owner) and not(lists:member(Node, Acc)) of
% 					true -> [Node | Acc];
% 					_ -> Acc
% 				end
% 			end,
% 			[],
% 			WrappedTable
% 		)
% 	),
% 	{Primaries, Secondaries} = prefs(Nodes, N_Val-1),
% 	[{primaries, Primaries}, {secondaries, Secondaries}].

% 	prefs(Nodes, N_Val) -> prefs(Nodes, N_Val, []).

% 	prefs(Secondaries, 0, Primaries) -> {Primaries, Secondaries};
% 	prefs([H|T], N_Val, Primaries) -> prefs(T, N_Val-1, [H | Primaries]).

% balance_table(Table, Indexes) ->
% 	Distribution = get_distribution(Table),
% 	Target = length(Distribution),
% 	Upper = sulibarri_dht_utils:ceiling(Target),
% 	Lower = sulibarri_dht_utils:floor(Target),

% 	fix_wrap(Indexes, Table, Distribution, Upper, Lower).
% fix_wrap()
% fix_wrap(Table, Indexes) ->
% 	Distribution = get_distribution(Table).
	% fix_wrap(Indexes, Table, Distribution).
% fix_wrap([Current_Index | Rest], Table, Distribution) ->

% get_neighbors(Partition_Id, Table) ->
% 	Window = [Partition_Id -2, Partition_Id -1, Partition_Id +1, Partition_Id + 2],
% 	% Window = lists:seq()
% 	Window2 = check_wrap(Window),
% 	Neighbors = neighbors(Window2, Table),
% 	Neighbors.

% neighbors(Window, Partition_Table) -> lists:reverse(neighbors(Window, Partition_Table, [])).
% neighbors([H | T], Partition_Table, Acc) ->
% 	neighbors(T, Partition_Table, [lists:nth(H, Partition_Table) | Acc]);
% neighbors([], _, Acc) -> Acc.

% get_distribution(Partition_Table) -> 
% 	Dist = lists:foldl(
% 			fun(Partition, Acc) ->
% 				{Node, _} = Partition,
% 				case lists:keyfind(Node, 1, Acc) of
% 					{_, Count} ->
% 						lists:keyreplace(Node, 1, Acc, {Node, Count + 1});	
% 					false ->
% 						[{Node, 1} | Acc]
% 				end
% 			end,
% 			[],
% 			Partition_Table),
% 	lists:reverse(Dist).

% lookup(Hash, Partition_Table) ->	
% 	lookup_node(Hash, Partition_Table).

% lookup_node(Hash, [H|T]) ->
% 	{_, {_, Top_Key}} = H,
% 	case Top_Key >= Hash of
% 		true ->
% 			H;
% 		false ->
% 			lookup_node(Hash, T)
% 	end.

