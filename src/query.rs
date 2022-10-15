
use crate::types::*;
use itertools::Itertools;
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;

// Patch

pub async fn patch(entity_id: &str, patches: &Vec<Patch>, pool: &bb8::Pool<bb8_redis::RedisConnectionManager>) -> Result<(), String> {
  let now = chrono::Utc::now().timestamp_millis();

  let mut conn = pool.get().await.map_err(|e| e.to_string())?;

  multi(&mut *conn).await?;

  for patch in patches {
    let bin = rmp_serde::encode::to_vec(&(now, patch)).map_err(|e| e.to_string())?;

    redis::cmd("RPUSH")
      .arg(mk_field_key(entity_id, &patch.field))
      .arg(bin)
      .query_async(&mut *conn)
      .await
      .map_err(|e| e.to_string())?;
  }

  exec(&mut *conn).await
}

pub async fn delete(entity_id: &str, pool: &bb8::Pool<bb8_redis::RedisConnectionManager>) -> Result<(), String> {
  let mut conn = pool.get().await.map_err(|e| e.to_string())?;

  let keys : Vec<String> =
    redis::cmd("KEYS")
    .arg(mk_fields_wildcard(entity_id))
    .query_async(&mut *conn)
    .await
    .map_err(|e| e.to_string())?;

  multi(&mut *conn).await?;

  for key in keys {
    redis::cmd("DEL")
      .arg(key)
      .query_async(&mut *conn)
      .await
      .map_err(|e| e.to_string())?;
  }

  exec(&mut *conn).await
}

// View

pub async fn view<'a>(
  entity_id: &'a str,
  views: &'a Vec<View>,
  pool: &bb8::Pool<bb8_redis::RedisConnectionManager>) -> Result<HashMap<&'a str, Value>, String> {
  let patch_map = build_patch_map(entity_id, views, pool).await?;

  views
    .par_iter()
    .map(|view| {
      match patch_map.get(&view.field[..]) {
        Some(patches) => {
          proj(&view, patches).map(|val| {
            let label = view.alias.as_ref().unwrap_or(&view.field);
            ViewResult::create(label, val).to_tuple()
          })
        },
        None => Err("Unable to find patches".to_string())
      }
    })
    .collect()
}

async fn build_patch_map<'a>(
  entity_id: &'a str,
  views: &'a Vec<View>,
  pool: &bb8::Pool<bb8_redis::RedisConnectionManager>) -> Result<HashMap<&'a str, Vec<(u64, Patch)>>, String> {
  let mut patch_map : HashMap<&'a str, Vec<(u64, Patch)>> = HashMap::new();

  let mut conn = pool.get().await.map_err(|e| e.to_string())?;

  let fields : Vec<&'a String> = views.iter().map(|v| &v.field).unique().collect();

  for field in fields {
    let raw : Vec<Vec<u8>> =
      redis::cmd("LRANGE")
      .arg(mk_field_key(entity_id, field))
      .arg(0)
      .arg(-1)
      .query_async(&mut *conn)
      .await
      .map_err(|e| e.to_string())?;

    let patches : Vec<(u64, Patch)> =
      raw
      .par_iter()
      .map(|j| rmp_serde::decode::from_read(&**j).map_err(|e| e.to_string()))
      .collect::<Result<Vec<(u64, Patch)>, String>>()?;

    patch_map.insert(field, patches);
  }

  Ok(patch_map)
}

fn proj<'a>(view: &'a View, patches: &'a Vec<(u64, Patch)>) -> Result<Value, String> {
  let patches = apply_filters(&view, patches);

  match &view.projection {
    Projection::Latest => Ok(latest(&patches)),
    Projection::Collect => Ok(collect(&patches)),
    Projection::Avg => avg(&patches),
    Projection::Sum => sum(&patches),
    Projection::Concat(sep) => concat(&patches, &sep),
    Projection::All => all(&patches),
    Projection::Any => any(&patches),
    Projection::None => none(&patches)
  }
}

fn apply_filters<'a>(view: &View, patches: &'a Vec<(u64, Patch)>) -> Vec<&'a Patch> {
  match &view.range {
    Some(Range { from, to }) => {
      patches
        .par_iter()
        .filter(|(t, p)| p.field == view.field && t >= from && t <= to)
        .map(|(_, p)| p)
        .collect()
    },
    None => {
      patches
        .par_iter()
        .filter(|(_, p)| p.field == view.field)
        .map(|(_, p)| p)
        .collect()
    }
  }
}

// Generic projections

fn latest(patches: &Vec<&Patch>) -> Value {
  match patches.last() {
    Some(p) => serde_json::to_value(&p.value).unwrap(),
    None => serde_json::to_value(()).unwrap()
  }
}

fn collect(patches: &Vec<&Patch>) -> Value {
  let vals : Vec<&Value> = patches.iter().map(|p| &p.value).collect();
  serde_json::to_value(vals).unwrap()
}

// Numeric projections

fn avg(patches: &Vec<&Patch>) -> Result<Value, String> {
  let nx = numerics(patches, "Cannot average non-numeric value stream".to_string())?;
  let res = nx.iter().sum::<f64>() / nx.len() as f64;
  Ok(serde_json::to_value(res).unwrap())
}

fn sum(patches: &Vec<&Patch>) -> Result<Value, String> {
  let nx = numerics(patches, "Cannot sum non-numeric value stream".to_string())?;
  let res = nx.iter().sum::<f64>();
  Ok(serde_json::to_value(res).unwrap())
}

// String projections

fn concat(patches: &Vec<&Patch>, sep: &str) -> Result<Value, String> {
  let sx = strings(patches, "Cannot concat non-string value stream".to_string())?;
  Ok(serde_json::to_value(sx.join(sep)).unwrap())
}

// Boolean projections

fn all(patches: &Vec<&Patch>) -> Result<Value, String> {
  let bx = bools(patches, "Cannot apply conjunction to non-boolean value stream".to_string())?;
  let res = bx.iter().all(|b| *b);
  Ok(serde_json::to_value(res).unwrap())
}

fn any(patches: &Vec<&Patch>) -> Result<Value, String> {
  let bx = bools(patches, "Cannot apply disjunction to non-boolean value stream".to_string())?;
  let res = bx.iter().any(|b| *b);
  Ok(serde_json::to_value(res).unwrap())
}

fn none(patches: &Vec<&Patch>) -> Result<Value, String> {
  let bx = bools(patches, "Cannot apply conjunction to non-boolean value stream".to_string())?;
  let res = bx.iter().all(|b| !(*b));
  Ok(serde_json::to_value(res).unwrap())
}

fn numerics(patches: &Vec<&Patch>, err: String) -> Result<Vec<f64>, String> {
  of_type(patches, |p| p.value.as_f64(), err)
}

fn strings<'a>(patches: &'a Vec<&Patch>, err: String) -> Result<Vec<&'a str>, String> {
  of_type(patches, |p| p.value.as_str(), err)
}

fn bools<'a>(patches: &'a Vec<&Patch>, err: String) -> Result<Vec<bool>, String> {
  of_type(patches, |p| p.value.as_bool(), err)
}

fn of_type<'a, R, F>(
  patches: &'a Vec<&Patch>,
  mut f: F,
  err: String) -> Result<Vec<R>, String> where F : FnMut(&'a Patch) -> Option<R> {
  let rx : Vec<R> = patches.iter().filter_map(|p| f(p)).collect();

  if rx.len() != patches.len() {
    Err(err)
  } else {
    Ok(rx)
  }
}

// Utils

async fn multi<C>(conn: &mut C) -> Result<(), String> where C : redis::aio::ConnectionLike {
  redis::cmd("MULTI")
    .query_async(conn)
    .await
    .map_err(|e| e.to_string())
}

async fn exec<C>(conn: &mut C) -> Result<(), String> where C : redis::aio::ConnectionLike {
  redis::cmd("EXEC")
    .query_async(conn)
    .await
    .map_err(|e| e.to_string())
}

fn mk_field_key(entity_id: &str, field: &str) -> String {
  format!("_{}-{}", entity_id, field)
}

fn mk_fields_wildcard(entity_id: &str) -> String {
  format!("_{}-*", entity_id)
}
