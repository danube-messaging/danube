# danube-reliable-dispatch

The danube-reliable-dispatch crate is responsible for ensuring reliable message delivery in Danube by managing message persistence through segmented storage, supporting different storage backends (in-memory, disk, S3) and implementing message acknowledgment tracking to guarantee at-least-once delivery semantics.
