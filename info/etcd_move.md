etcdctl watch / --prefix

PUT
/cluster/unassigned/default/reliable_topic
null
PUT
/namespaces/default/topics/default/reliable_topic
null
PUT
/cluster/brokers/10285063371164059634/default/reliable_topic
null
PUT
/topics/default/reliable_topic/delivery
"Reliable"
DELETE
/cluster/unassigned/default/reliable_topic


PUT
/topics/default/reliable_topic/producers/5016883522339102726
{"access_mode":0,"producer_id":5016883522339102726,"producer_name":"prod_json_reliable","status":true,"topic_name":"/default/reliable_topic"}
PUT
/topics/default/reliable_topic/subscriptions/subs_reliable
{"consumer_id":null,"consumer_name":"cons_reliable","subscription_name":"subs_reliable","subscription_type":0}
PUT
/topics/default/reliable_topic/subscriptions/subs_reliable/cursor
13

PUT
/danube-data/storage/topics/default/reliable_topic/objects/00000000000000000000
{"completed":true,"created_at":1768625209,"end_offset":21,"etag":null,"object_id":"data-0-21.dnb1","offset_index":[[0,0]],"size":5252,"start_offset":0}
PUT
/danube-data/storage/topics/default/reliable_topic/objects/cur
{"start":"00000000000000000000"}


PUT
/cluster/unassigned/default/reliable_topic
{"from_broker":10285063371164059634,"reason":"unload"}
DELETE
/cluster/brokers/10285063371164059634/default/reliable_topic

PUT
/cluster/brokers/10421046117770015389/default/reliable_topic
null
PUT
/danube-data/storage/topics/default/reliable_topic/state
{"broker_id":10285063371164059634,"last_committed_offset":21,"sealed":true,"timestamp":1768625254}

DELETE
/cluster/unassigned/default/reliable_topic

DELETE
/topics/default/reliable_topic/producers/5016883522339102726

PUT
/topics/default/reliable_topic/producers/12096190879935223134
{"access_mode":0,"producer_id":12096190879935223134,"producer_name":"prod_json_reliable","status":true,"topic_name":"/default/reliable_topic"}
PUT
/topics/default/reliable_topic/subscriptions/subs_reliable
{"consumer_id":null,"consumer_name":"cons_reliable","subscription_name":"subs_reliable","subscription_type":0}
PUT
/topics/default/reliable_topic/subscriptions/subs_reliable/cursor
14

PUT
/danube-data/storage/topics/default/reliable_topic/objects/00000000000000000000
{"completed":true,"created_at":1768625284,"end_offset":20,"etag":null,"object_id":"data-0-20.dnb1","offset_index":[[0,0]],"size":5028,"start_offset":0}
PUT
/danube-data/storage/topics/default/reliable_topic/objects/cur
{"start":"00000000000000000000"}

PUT
/danube-data/storage/topics/default/reliable_topic/objects/00000000000000000021
{"completed":true,"created_at":1768625314,"end_offset":22,"etag":null,"object_id":"data-21-22.dnb1","offset_index":[[21,0]],"size":466,"start_offset":21}
PUT
/danube-data/storage/topics/default/reliable_topic/objects/cur
{"start":"00000000000000000021"}
