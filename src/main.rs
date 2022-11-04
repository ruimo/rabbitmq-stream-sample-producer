use clap::Parser;
use model::User;
use rabbitmq_stream_client::{Environment, types::Message};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value="mystream")]
    stream_name: String,
    #[arg(short, long)]
    create_stream: bool,
    user_name: String,
    email: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let env = Environment::builder().build().await.unwrap();
    create_stream(&args, &env).await;

    let user = User { user_name: args.user_name.clone(), email: args.email.clone() };
    send(&args, &env, &user).await;
}

async fn create_stream(args: &Args, env: &Environment) {
    if args.create_stream {
        env.stream_creator().create(&args.stream_name).await.expect("Cannot create stream")
    }
}

async fn send(args: &Args, env: &Environment, user: &User) {
    let producer = env.producer().build(&args.stream_name).await.expect("Cannot create producer");
    let bytes = User::serialize(user).expect("Cannot serialize user");
    producer
        .send_with_confirm(Message::builder().body(bytes).build())
        .await.unwrap();

    producer.close().await.unwrap();
}
