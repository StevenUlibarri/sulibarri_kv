-module(sulibarri_dht_client).
-export([join/1, put/2, get/1, delete/1]).


join(Node) ->
	sulibarri_dht_node:join(Node).

put(Key, Value) ->
	sulibarri_dht_node:put(Key, Value).

get(Key) ->
	sulibarri_dht_node:get(Key).

delete(Key) ->
	sulibarri_dht_node:delete(Key).




