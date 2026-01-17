cargo run --example reliable_dispatch_producer 
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.16s
     Running `/home/rdan/my_projects/danube/target/debug/examples/reliable_dispatch_producer`
The Producer prod_json_reliable was created
The Message with id 4 was sent
The Message with id 5 was sent
The Message with id 6 was sent
The Message with id 7 was sent
The Message with id 8 was sent

cargo run --example reliable_dispatch_consumer 
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.16s
     Running `/home/rdan/my_projects/danube/target/debug/examples/reliable_dispatch_consumer`
The Consumer cons_reliable was created
Received message: Danube messaging service manages requests quickly, with low latency and high throughput. It is designed for reliability, with real-time. | offset: 6 | total received bytes: 136
Received message: The Danube application delivers events seamlessly, with low latency and high throughput. It is designed for reliability, without issues. | offset: 7 | total received bytes: 272
Received message: The Danube system processes messages efficiently, with low latency and high throughput. It is designed for reliability, with high performance. | offset: 8 | total received bytes: 414
....
....
eceived message: The Danube system processes messages efficiently, with low latency and high throughput. It is designed for reliability, with high performance. | offset: 20 | total received bytes: 1182
Received message: Danube platform handles data reliably, with low latency and high throughput. It is designed for reliability, at scale. | offset: 21 | total received bytes: 1300


./target/debug/danube-admin-cli topics unload /default/reliable_topic - so the topic moved to another broker

 cargo run --example reliable_dispatch_producer 
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.19s
     Running `/home/rdan/my_projects/danube/target/debug/examples/reliable_dispatch_producer`
The Producer prod_json_reliable was created
The Message with id 2 was sent
The Message with id 3 was sent
The Message with id 4 was sent
The Message with id 5 was sent
The Message with id 6 was sent
The Message with id 7 was sent


THE ISSUE, does not start immediately, but after some time:

cargo run --example reliable_dispatch_consumer 
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.16s
     Running `/home/rdan/my_projects/danube/target/debug/examples/reliable_dispatch_consumer`
The Consumer cons_reliable was created
Received message: Danube messaging service manages requests quickly, with low latency and high throughput. It is designed for reliability, with real-time. | offset: 14 | total received bytes: 136
Received message: The Danube application delivers events seamlessly, with low latency and high throughput. It is designed for reliability, without issues. | offset: 15 | total received bytes: 272
Received message: The Danube system processes messages efficiently, with low latency and high throughput. It is designed for reliability, with high performance. | offset: 16 | total received bytes: 414
