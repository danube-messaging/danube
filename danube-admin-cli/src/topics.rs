use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    topic_admin_client::TopicAdminClient, NamespaceRequest, NewTopicRequest, SubscriptionRequest,
    TopicRequest,
};

#[derive(Debug, Args)]
pub(crate) struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCommands {
    #[command(about = "List topics in the specified namespace")]
    List { namespace: String },
    #[command(about = "Create a non-partitioned topic")]
    Create {
        topic: String,
        #[arg(short, long, default_value = "String")]
        schema_type: String,
        #[arg(short = 'd', long, default_value = "{}")]
        schema_data: String,
        #[arg(short, long, default_value = "non_reliable")]
        dispatch_strategy: String,
    },
    #[command(about = "Create a partitioned topic")]
    CreatePartitioned {
        topic: String,
        partitions: usize,
        #[arg(short, long, default_value = "String")]
        schema_type: String,
        #[arg(short = 'd', long, default_value = "{}")]
        schema_data: String,
        #[arg(short, long, default_value = "non_reliable")]
        dispatch_strategy: String,
    },
    #[command(about = "Delete an existing topic")]
    Delete { topic: String },
    #[command(about = "List the subscriptions of the specified topic")]
    Subscriptions { topic: String },
    #[command(about = "Delete a subscription from a topic")]
    Unsubscribe {
        topic: String,
        #[arg(short, long)]
        subscription: String,
    },
}

#[allow(unreachable_code)]
pub async fn handle_command(topics: Topics) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = TopicAdminClient::connect("http://127.0.0.1:50051").await?;

    match topics.command {
        // Get the list of topics of a namespace
        TopicsCommands::List { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.list_topics(request).await?;

            let topics = response.into_inner().topics;

            for topic in topics {
                println!("Topic: {}", topic);
            }
        }

        // Creates a non-partitioned topic
        TopicsCommands::Create {
            topic,
            mut schema_type,
            schema_data,
            dispatch_strategy,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            if schema_type.is_empty() {
                schema_type = "String".into()
            }
            let request = NewTopicRequest {
                name: topic,
                schema_type,
                schema_data,
                dispatch_strategy: dispatch_strategy,
            };
            let response = client.create_topic(request).await?;
            println!("Topic Created: {:?}", response.into_inner().success);
        }

        // Creates a partitioned topic (should specify the number of partitions)
        TopicsCommands::CreatePartitioned {
            topic,
            partitions,
            mut schema_type,
            schema_data,
            dispatch_strategy,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            if schema_type.is_empty() {
                schema_type = "String".into()
            }

            let topic_requests: Vec<NewTopicRequest> = (0..partitions)
                .map(|partition_id| {
                    let topic = format!("{}-part-{}", topic, partition_id);
                    NewTopicRequest {
                        name: topic,
                        schema_type: schema_type.clone(),
                        schema_data: schema_data.clone(),
                        dispatch_strategy: dispatch_strategy.clone(),
                    }
                })
                .collect();

            for topic_req in topic_requests {
                let response = client.create_topic(topic_req).await?;
                println!("Topic Created: {:?}", response.into_inner().success);
            }
        }

        // Delete the topic
        TopicsCommands::Delete { topic } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }

        // Get the list of subscriptions on the topic
        TopicsCommands::Subscriptions { topic } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.list_subscriptions(request).await?;
            println!("Subscriptions: {:?}", response.into_inner().subscriptions);
        }

        // Delete a subscription from a topic
        TopicsCommands::Unsubscribe {
            topic,
            subscription,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = SubscriptionRequest {
                topic,
                subscription,
            };
            let response = client.unsubscribe(request).await?;
            println!("Unsubscribed: {:?}", response.into_inner().success);
        }
    }

    Ok(())
}

// Topics string representation:  /{namespace}/{topic-name}
pub(crate) fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}
