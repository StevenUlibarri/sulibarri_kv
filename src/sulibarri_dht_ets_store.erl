-module(sulibarri_dht_ets_store).

-export([init/0, put/2, get/1, delete/1, all/0, bulk_insert/1]).

-define(TABLE_ID, dht_ets).

init() ->
	lager:info("Local storage initialized."),
	ets:new(?TABLE_ID, [named_table, public, set]),
	ok.

put(Key, Value) ->
	ets:insert(?TABLE_ID, {Key, Value}).

get(Key) ->
	case ets:lookup(?TABLE_ID, Key) of
		[{Key, Value}] -> {ok, Value};
		[] -> {error, not_found}
	end.

delete(Key) ->
	ets:delete(?TABLE_ID, Key).

all() ->
	ets:tab2list(?TABLE_ID).

bulk_insert(Data) ->
	ets:insert(?TABLE_ID, Data).


