use actix::prelude::*;
use env_logger;
use zqtt::broker::server;

#[actix_rt::main]
async fn main() {
    env_logger::init();

    let addr = "127.0.0.1:1883";
    server::Server::run(addr).await;
    println!("Running chat server on {} ...", addr);

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    System::current().stop();
}
