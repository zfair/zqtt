#[macro_use]
extern crate log;
extern crate env_logger;

use actix::prelude::*;
use zqtt::broker::server;

#[actix_rt::main]
async fn main() {
    env_logger::init();
    let server = server::Server::run().await;

    println!("Running chat server on 127.0.0.1:12345");

    tokio::signal::ctrl_c().await.unwrap();
    println!("Ctrl-C received, shutting down");
    // server.stop().await;
    System::current().stop();
}