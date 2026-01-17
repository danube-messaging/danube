broker id:14388677249660776213

2026-01-17T04:46:19.040356Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:46:19.043172Z  INFO danube_broker::danube_service::load_manager: attempting to assign the new topic to a broker topic=/cluster/unassigned/default/reliable_topic
2026-01-17T04:46:19.052860Z  INFO danube_broker::danube_service::load_manager: the topic was successfully assigned to broker topic=/default/reliable_topic broker_id=10285063371164059634
2026-01-17T04:46:24.909849Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable
2026-01-17T04:46:38.497421Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable
2026-01-17T04:47:34.591243Z  INFO danube_broker::danube_service::load_manager: attempting to assign the new topic to a broker topic=/cluster/unassigned/default/reliable_topic
2026-01-17T04:47:34.619740Z  INFO danube_broker::danube_service::load_manager: the topic was successfully assigned to broker topic=/default/reliable_topic broker_id=10421046117770015389
2026-01-17T04:47:43.635877Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:47:47.390526Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable

broker id: 10285063371164059634

2026-01-17T04:46:19.052780Z  INFO danube_broker::danube_service::broker_watcher: a new watch event has been received Put(key: /cluster/brokers/10285063371164059634/default/reliable_topic, mod_revision: 45, version: 1)
2026-01-17T04:46:19.203940Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:46:19.454620Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:46:19.557046Z  INFO danube_persistent_storage::wal: initialized WAL file target="wal" wal_file=./danube-data/wal/default/reliable_topic/wal.log
2026-01-17T04:46:19.557114Z  INFO danube_persistent_storage::wal: WAL configuration applied target="wal" wal_dir=./danube-data/wal/default/reliable_topic cache_capacity=1024 fsync_interval_ms=5000 fsync_max_batch_bytes=10485760 rotate_max_bytes=536870912 rotate_max_seconds=0 checkpoint=./danube-data/wal/default/reliable_topic/wal.ckpt
2026-01-17T04:46:19.557179Z  INFO danube_persistent_storage::wal_factory: created per-topic WAL target="wal_factory" topic=default/reliable_topic wal_dir=./danube-data/wal/default/reliable_topic
2026-01-17T04:46:19.557201Z  INFO danube_persistent_storage::wal_factory: starting per-topic uploader target="wal_factory" topic=default/reliable_topic
2026-01-17T04:46:19.557272Z  INFO danube_persistent_storage::wal_factory: starting per-topic deleter target="wal_factory" topic=default/reliable_topic
2026-01-17T04:46:19.557288Z  INFO danube_persistent_storage::wal_storage: cloud handoff enabled for topic target="wal_storage" topic=default/reliable_topic
2026-01-17T04:46:19.557357Z  INFO danube_broker::topic_worker: worker adding topic worker_id=3 topic=/default/reliable_topic
2026-01-17T04:46:19.557389Z  INFO danube_broker::danube_service::broker_watcher: topic created on broker topic=/default/reliable_topic broker_id=10285063371164059634 strategy=Reliable schema=none
2026-01-17T04:46:19.557432Z  INFO danube_persistent_storage::cloud::uploader: uploader started target="uploader" topic=default/reliable_topic interval=30
2026-01-17T04:46:19.557482Z  INFO danube_persistent_storage::cloud::uploader: resumed uploader from checkpoint target="uploader" last_committed_offset=42 last_object_id="data-30-42.dnb1"
2026-01-17T04:46:20.035840Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:46:20.044825Z  INFO create_producer: danube_broker::broker_server::producer_handler: producer successfully created producer_id=5016883522339102726 producer_name=prod_json_reliable topic=/default/reliable_topic
2026-01-17T04:46:25.107302Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable
2026-01-17T04:46:25.108947Z  INFO danube_persistent_storage::wal_storage: creating reader from WAL only (request is within local retention) target="wal_storage" topic=default/reliable_topic start=6 wal_start=0
2026-01-17T04:46:25.112885Z  INFO subscribe: danube_broker::broker_server::consumer_handler: consumer successfully created consumer_id=3089935414709568615 consumer_name=cons_reliable subscription=subs_reliable topic=/default/reliable_topic
2026-01-17T04:46:25.116874Z  INFO receive_messages: danube_broker::broker_server::consumer_handler: consumer ready to receive messages consumer_id=3089935414709568615
2026-01-17T04:46:32.996590Z  WARN danube_broker::broker_server::consumer_handler: Client disconnected, marking consumer inactive consumer_id=3089935414709568615
2026-01-17T04:46:38.615801Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable
2026-01-17T04:46:38.620762Z  INFO receive_messages: danube_broker::broker_server::consumer_handler: consumer ready to receive messages consumer_id=3089935414709568615
2026-01-17T04:46:42.812864Z  WARN danube_broker::broker_server::consumer_handler: Client disconnected, marking consumer inactive consumer_id=3089935414709568615
2026-01-17T04:47:34.615081Z  INFO danube_broker::danube_service::broker_watcher: a new watch event has been received Delete(key: /cluster/brokers/10285063371164059634/default/reliable_topic, mod_revision: 62, version: 0)
2026-01-17T04:47:34.618366Z  INFO danube_persistent_storage::cloud::uploader: uploader stopped after drain (cancel) target="uploader" topic=default/reliable_topic
2026-01-17T04:47:34.618480Z  INFO danube_persistent_storage::wal::deleter: deleter stopping by cancel target="wal_deleter" topic=default/reliable_topic
2026-01-17T04:47:34.627483Z  INFO danube_broker::danube_service::broker_watcher: Topic unloaded locally topic=/default/reliable_topic broker_id=10285063371164059634

broker id:10421046117770015389

2026-01-17T04:47:34.619737Z  INFO danube_broker::danube_service::broker_watcher: a new watch event has been received Put(key: /cluster/brokers/10421046117770015389/default/reliable_topic, mod_revision: 63, version: 1)
2026-01-17T04:47:34.625258Z  INFO danube_persistent_storage::wal: initialized WAL file target="wal" wal_file=./danube-data/wal/default/reliable_topic/wal.log
2026-01-17T04:47:34.625298Z  INFO danube_persistent_storage::wal: WAL configuration applied target="wal" wal_dir=./danube-data/wal/default/reliable_topic cache_capacity=1024 fsync_interval_ms=5000 fsync_max_batch_bytes=10485760 rotate_max_bytes=536870912 rotate_max_seconds=0 checkpoint=./danube-data/wal/default/reliable_topic/wal.ckpt
2026-01-17T04:47:34.625351Z  INFO danube_persistent_storage::wal_factory: created per-topic WAL target="wal_factory" topic=default/reliable_topic wal_dir=./danube-data/wal/default/reliable_topic
2026-01-17T04:47:34.625370Z  INFO danube_persistent_storage::wal_factory: starting per-topic uploader target="wal_factory" topic=default/reliable_topic
2026-01-17T04:47:34.625413Z  INFO danube_persistent_storage::wal_factory: starting per-topic deleter target="wal_factory" topic=default/reliable_topic
2026-01-17T04:47:34.625428Z  INFO danube_persistent_storage::wal_storage: cloud handoff enabled for topic target="wal_storage" topic=default/reliable_topic
2026-01-17T04:47:34.625463Z  INFO danube_persistent_storage::cloud::uploader: uploader started target="uploader" topic=default/reliable_topic interval=30
2026-01-17T04:47:34.625471Z  INFO danube_broker::topic_worker: worker adding topic worker_id=3 topic=/default/reliable_topic
2026-01-17T04:47:34.625481Z  INFO danube_persistent_storage::cloud::uploader: resumed uploader from checkpoint target="uploader" last_committed_offset=21 last_object_id="data-0-21.dnb1"
2026-01-17T04:47:34.625497Z  INFO danube_broker::danube_service::broker_watcher: topic created on broker topic=/default/reliable_topic broker_id=10421046117770015389 strategy=Reliable schema=none
2026-01-17T04:47:43.756535Z  INFO create_producer: danube_broker::broker_server::producer_handler: received producer creation request producer_name=prod_json_reliable topic=/default/reliable_topic dispatch_strategy=Reliable has_schema=false
2026-01-17T04:47:43.765435Z  INFO create_producer: danube_broker::broker_server::producer_handler: producer successfully created producer_id=12096190879935223134 producer_name=prod_json_reliable topic=/default/reliable_topic
2026-01-17T04:47:47.528283Z  INFO subscribe: danube_broker::broker_server::consumer_handler: received consumer creation request consumer_name=cons_reliable topic=/default/reliable_topic subscription_type=0 subscription=subs_reliable
2026-01-17T04:47:47.531936Z  INFO danube_persistent_storage::wal_storage: creating reader from WAL only (request is within local retention) target="wal_storage" topic=default/reliable_topic start=14 wal_start=0
2026-01-17T04:47:47.532019Z  INFO danube_persistent_storage::wal::stateful_reader: replay decision: using cache, then live (skip files) target="stateful_reader" from_offset=14 cache_start=0 decision="Cache→Live"
2026-01-17T04:47:47.533294Z  INFO danube_persistent_storage::wal::stateful_reader: cache exhausted (or pending refill), transitioning to live target="stateful_reader" last_yielded=13
2026-01-17T04:47:47.535979Z  INFO subscribe: danube_broker::broker_server::consumer_handler: consumer successfully created consumer_id=8849981167183788072 consumer_name=cons_reliable subscription=subs_reliable topic=/default/reliable_topic
2026-01-17T04:47:47.538180Z  INFO receive_messages: danube_broker::broker_server::consumer_handler: consumer ready to receive messages consumer_id=8849981167183788072
2026-01-17T04:48:00.851259Z  WARN danube_broker::broker_server::consumer_handler: Client disconnected, marking consumer inactive consumer_id=8849981167183788072
