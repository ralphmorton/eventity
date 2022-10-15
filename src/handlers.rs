use super::query;
use crate::types::*;
use rocket::serde::json::Json;
use rocket::State;

#[patch("/<entity_id>", format = "json", data = "<patches>")]
pub async fn patch(
    entity_id: &str,
    patches: Json<Vec<Patch>>,
    pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>,
) -> Result<(), String> {
    ensure_valid_entity_id(entity_id)?;

    query::patch(entity_id, &patches, pool).await
}

#[delete("/<entity_id>", format = "json")]
pub async fn delete(
    entity_id: &str,
    pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>,
) -> Result<(), String> {
    ensure_valid_entity_id(entity_id)?;

    query::delete(entity_id, pool).await
}

#[post("/<entity_id>", format = "json", data = "<views>")]
pub async fn view(
    entity_id: &str,
    views: Json<Vec<View>>,
    pool: &State<bb8::Pool<bb8_redis::RedisConnectionManager>>,
) -> Result<ViewResponse, String> {
    ensure_valid_entity_id(entity_id)?;

    let vr = query::view(entity_id, &views, pool).await;
    let response = ViewResponse::create(vr);

    Ok(response)
}

fn ensure_valid_entity_id(entity_id: &str) -> Result<(), String> {
    if entity_id.chars().next() == Some('_') {
        return Err("Entity IDs cannot begin with _".to_string());
    }

    Ok(())
}
