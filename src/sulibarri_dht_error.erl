-module(sulibarri_dht_error).

-export([make_error/3]).

make_error(Op, Args, Reason) ->
	{error, [
		{op, Op},
		{args, Args},
		{reason, Reason}
	]}.

