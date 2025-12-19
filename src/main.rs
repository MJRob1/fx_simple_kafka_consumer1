mod consumer;

#[tokio::main]
async fn main() {
    consumer::start().await;
}
