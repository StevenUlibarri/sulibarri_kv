-module(sulibarri_dht_client).

-compile([export_all]).

reply(Origin, Message) ->
	gen_server:cast({sulibarri_dht_client, Origin}, {reply, Message}).