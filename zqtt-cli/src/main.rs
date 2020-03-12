use actix::prelude::*;
use structopt::StructOpt;
use zqtt::broker::server;

const DEFAULT_SERVER_ADDR: &str = "127.0.0.1:1883";

#[derive(Debug, StructOpt)]
#[structopt(name = "zqtt-cli", about = "Zqtt, a fast and safe MQTT broker")]
struct Opt {
    #[structopt(name = "ADDR", help = "Server address", default_value = DEFAULT_SERVER_ADDR)]
    addr: String,
}

#[actix_rt::main]
async fn main() {
    let opt = Opt::from_args();
    env_logger::init();

    server::Server::run(&opt.addr).await;
    println!("Running chat server on {} ...", opt.addr);

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    System::current().stop();
}
