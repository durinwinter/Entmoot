//! Minimal MQTT publisher for poking at a local mesh without mosquitto.
//! cargo run -p entmoot-node --example pub -- --port 1885 --topic plant/kiln1/temp --msg 993.5

use clap::Parser;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 1883)]
    port: u16,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    msg: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut opts = MqttOptions::new(format!("entmoot-pub-{}", std::process::id()), args.host, args.port);
    opts.set_keep_alive(Duration::from_secs(15));
    let (client, mut eventloop) = AsyncClient::new(opts, 16);
    client
        .publish(&args.topic, QoS::AtLeastOnce, false, args.msg.as_bytes())
        .await
        .unwrap();
    // Drive the event loop until the broker acks the publish.
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::PubAck(_))) => {
                println!("published to {:?}", args.topic);
                return;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("connection error: {e}");
                std::process::exit(1);
            }
        }
    }
}
