-record(object, {
	key :: term(),
	values = [],
	clock = [] :: sulibarri_dht_vclock:vclock(),
	deleted = false :: boolean()
	}).