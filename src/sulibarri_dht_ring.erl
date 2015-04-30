-module(sulibarri_dht_ring).
% -compile([export_all,     
% debug_info]).

-define(MAX_INDEX, (math:pow(2,160)-1)).
-define(DEFAULT_PARTITIONS, 64).

-export([
		brand_new_ring/0,
		get_new_ring/3,
		hash/1,
		lookup/2,
		get_transfers/3
		]).

hash(Key) ->
	ByteHash = crypto:sha(Key),
	Hash = crypto:bytes_to_integer(ByteHash),
	Hash.

brand_new_ring() ->
	diagonalized_ring([node()]).

get_new_ring(Current_Nodes, New_Node, Partition_Table) ->
	new_ring(Current_Nodes ++ [New_Node], length(Current_Nodes)+1, Partition_Table).

new_ring(Nodes, Num_Nodes, _) when Num_Nodes =< 3 ->
	diagonalized_ring(Nodes);
new_ring(Nodes, Num_Nodes, _) when Num_Nodes =:= 4 ->
	diagonalized_ring(Nodes);
new_ring(Nodes, Num_Nodes, Partition_Table) when Num_Nodes > 4 ->
	balance_ring(Nodes, Partition_Table).

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

	{Current_Index, {Current_Node_Name, Hash}} = lists:keyfind(Current_Index, 1, Table),
	{New_Node_Name, New_Node_Count} = New_Node,

	Highest = get_highest_dist([New_Node | Distribution]),

	Can_Take = can_take(Current_Node_Name, Distribution, Lower, Highest),
	Can_Fit = can_fit(New_Node_Name, Current_Index, Table),

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

diagonalized_ring(Nodes) ->
	Partitions = lists:seq(1, ?DEFAULT_PARTITIONS),
	Partition_Table = stripe(Partitions, Nodes),
	Partition_Table.

can_fit(Node, Partition_Id, Partition_Table) ->
	Neighbors = get_neighbors(Partition_Id, Partition_Table),
	Neighbor_Nodes = [N || {_, {N, _}} <- Neighbors],
	case lists:dropwhile(fun(Neighbor_Node) -> Neighbor_Node =/= Node end, Neighbor_Nodes) of
		[] -> true;
		_ -> false
	end.

get_neighbors(Partition_Id, Partition_Table) ->
	Window = [Partition_Id -2, Partition_Id -1, Partition_Id +1, Partition_Id + 2],
	Window2 = check_wrap(Window),
	Neighbors = neighbors(Window2, Partition_Table),
	Neighbors.

neighbors(Window, Partition_Table) -> lists:reverse(neighbors(Window, Partition_Table, [])).
neighbors([H | T], Partition_Table, Acc) ->
	neighbors(T, Partition_Table, [lists:keyfind(H, 1, Partition_Table) | Acc]);
	neighbors([], _, Acc) -> Acc.

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

stripe(L, R) -> lists:reverse(stripe(L, R, R, [])).
stripe([A|L], [B|R], Right, Acc) -> 
	stripe(L, R, Right, [{A, {B, (?MAX_INDEX / ?DEFAULT_PARTITIONS) * A}}|Acc]); 
stripe([], _, _, Acc) ->
		Acc; 
stripe(L, [], Right, Acc) -> 
		stripe(L, Right, Right, Acc).

lookup(Hash, Partition_Table) ->	
	lookup_node(Hash, Partition_Table).

lookup_node(Hash, [H|T]) ->
	{_, {_, Top_Key}} = H,
	case Top_Key >= Hash of
		true ->
			H;
		false ->
			lookup_node(Hash, T)
	end.

get_distribution(Partition_Table) -> 
	Dist = lists:foldl(
			fun(Partition, Acc) ->
				{_, {Node, _}} = Partition,
				case lists:keyfind(Node, 1, Acc) of
					{_, Count} ->
						lists:keyreplace(Node, 1, Acc, {Node, Count + 1});	
					false ->
						[{Node, 1} | Acc]
				end
			end,
			[],
			Partition_Table),
	lists:reverse(Dist).

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

% get_pref_list(Partition_Id, Table, N_Val) ->
% 	{_ {Owner, _}} = lists:keyfind(Partition_Id, 1, Table),
% 	{H, T} = lists:splitwith(fun({N, _}) -> N =:= Partition_Id end, Table),
% 	WrappedTable = H ++ T,
% 	CleanedWrappedTable = lists:dropwhile(
% 		fun({_, {Node, _}}) -> Node =:=Owner end,
% 		WrappedTable
% 	),

% 	Num_Nodes = length(get_distribution(Table)),

% 	pref_list(CleanedWrappedTable, N_Val - 1, Num_Nodes)
