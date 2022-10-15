
use super::query;
use crate::types::*;
use rayon::prelude::*;
use rocket::serde::json::{Json, Value, json};
use rocket::State;

#[patch("/<entity_id>", format = "json", data = "<patches>")]
pub async fn patch(entity_id: &str, patches: Json<Vec<Patch>>, pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>) -> Value {
  let now = chrono::Utc::now().timestamp_millis();

  let serialized : Vec<_> =
    patches
    .iter()
    .map(|p| rmp_serde::encode::to_vec(&(now, p)).unwrap())
    .collect();

  let mut conn = pool.get().await.unwrap();

  let _ : i32 =
    redis::cmd("RPUSH")
    .arg(entity_id)
    .arg(serialized)
    .query_async(&mut *conn)
    .await
    .unwrap();

  json!(())
}

#[delete("/<entity_id>", format = "json")]
pub async fn delete(entity_id: &str, pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>) -> Value {
  let mut conn = pool.get().await.unwrap();

  let _ : i32 =
    redis::cmd("DEL")
    .arg(entity_id)
    .query_async(&mut *conn)
    .await
    .unwrap();

  json!(())
}

#[post("/<entity_id>", format="json", data="<vx>")]
pub async fn view(entity_id: &str, vx: Json<Vec<View>>, pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>) -> ViewResponse {
  let mut conn = pool.get().await.unwrap();

  let raw : Vec<Vec<u8>> =
    redis::cmd("LRANGE")
    .arg(entity_id)
    .arg(0)
    .arg(-1)
    .query_async(&mut *conn)
    .await
    .unwrap();

  let px : Vec<(u64, Patch)> = raw.par_iter().map(|j| rmp_serde::decode::from_read(&**j).unwrap()).collect();

  ViewResponse::create(query::run(&vx, &px))
}
