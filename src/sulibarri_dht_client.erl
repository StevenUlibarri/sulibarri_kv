-module(sulibarri_dht_client).
-export([cluster/0, cluster/1, put/2, get/1, delete/1, output/1]).

cluster() ->
	sulibarri_dht_node:cluster().

cluster(Node) ->
	sulibarri_dht_node:cluster(Node).

put(Key, Value) ->
	sulibarri_dht_node:put(node(), Key, Value).

get(Key) ->
	sulibarri_dht_node:get(node(), Key).

delete(Key) ->
	sulibarri_dht_node:delete(node(), Key).

output(Obj) ->
	io:format("~n~p~n", [Obj]).






