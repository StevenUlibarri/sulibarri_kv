-record(get_op, {key, sender}).
-record(local_put_op, {obj, sender}).
-record(replicate_put_op, {obj, sender}).

-define(GET_OP, #get_op).
-define(LOCAL_PUT_OP, #local_put_op).
-define(REPLICATE_PUT_OP, #replicate_put_op).