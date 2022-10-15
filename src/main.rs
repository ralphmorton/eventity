#[macro_use]
extern crate rocket;

use bb8_redis;
use bb8_redis::RedisConnectionManager;
use clap::Parser;
use eventity::handlers;
use rocket::config::Config;

#[launch]
async fn rocket() -> _ {
    let args = Args::parse();

    let manager = RedisConnectionManager::new(args.redis).unwrap();
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    let mut config = Config::debug_default();

    config.port = args.port;

    rocket::custom(config).manage(pool).mount(
        "/",
        routes![handlers::patch, handlers::delete, handlers::view],
    )
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Port to listen on
    #[arg(short, long)]
    pub port: u16,
    /// Redis connection string
    #[arg(short, long)]
    pub redis: String,
}
