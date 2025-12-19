use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};

pub async fn start() {
    println!("Consumer1 started");
    let consumer: StreamConsumer = crate::consumer::create();
    consume(consumer).await;
}

fn create() -> StreamConsumer {
    let mut binding = ClientConfig::new();
    let config = binding
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "group1")
        .set("auto.offset.reset", "latest")
        .set("socket.timeout.ms", "6000");

    let consumer: StreamConsumer = config.create().expect("Consumer1 creation error");
    consumer
}

async fn consume(consumer: StreamConsumer) {
    println!("Consumer1: consuming messages...");
    consumer
        .subscribe(&["my-topic"])
        .expect("Consumer1 can't subscribe to specified topic");

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Consumer1: Kafka error: {}", e),
            Ok(m) => {
                match m.payload_view::<str>() {
                    None => println!("No payload"),
                    Some(Ok(s)) => println!("Received message: {}", s),
                    Some(Err(e)) => println!(
                        "Consumer1: Error while deserializing message payload: {:?}",
                        e
                    ),
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        }
    }
}
