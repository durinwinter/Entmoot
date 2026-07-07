//! Minimal MQTT subscriber for poking at a local mesh without mosquitto.
//! cargo run -p entmoot-node --example sub -- --port 1883 --topic 'plant/#'

use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 1883)]
    port: u16,
    #[arg(long, default_value = "plant/#")]
    topic: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut opts = MqttOptions::new(format!("entmoot-sub-{}", std::process::id()), args.host, args.port);
    opts.set_keep_alive(Duration::from_secs(15));
    let (client, mut eventloop) = AsyncClient::new(opts, 16);
    client.subscribe(&args.topic, QoS::AtLeastOnce).await.unwrap();
    println!("subscribed to {:?}, waiting for messages (ctrl-c to quit)", args.topic);
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                println!("[{}] {}", p.topic, String::from_utf8_lossy(&p.payload));
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("connection error: {e}");
                std::process::exit(1);
            }
        }
    }
}
