-module(sulibarri_dht_hash).

-export([hash/1,
		  new_ring/1,
		  lookup/2]).

-define(MAX_INDEX, (math:pow(2,160)-1)).
-define(DEFAULT_PARTITIONS, 64).

hash(Key) ->
	ByteHash = crypto:sha(Key),
	Hash = crypto:bytes_to_integer(ByteHash),
	Hash.

new_ring(Nodes) ->
	Partitions = lists:seq(1, ?DEFAULT_PARTITIONS),
	PartitionTable = orddict:from_list(stripe(Partitions, [Nodes])),
	PartitionTable.

stripe(L, R) -> lists:reverse(stripe(L, R, R, [])).
stripe([A|L], [B|R], Right, Acc) -> 
	stripe(L, R, Right, [{A, {B, (?MAX_INDEX / ?DEFAULT_PARTITIONS) * A}}|Acc]); 
	stripe([], _, _, Acc) ->
		Acc; 
	stripe(L, [], Right, Acc) -> 
		stripe(L, Right, Right, Acc).

lookup(Hash, Partition_Table) ->
	Partition_List = orddict:to_list(Partition_Table),
	lookup_node(Hash, Partition_List).

lookup_node(Hash, [H|T]) ->
	{_, {_, Top_Key}} = H,
	case Top_Key >= Hash of
		true ->
			H;
		false ->
			lookup_node(Hash, T)
	end.