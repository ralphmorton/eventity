
use crate::types::*;
use serde_json::Value;
use std::collections::HashMap;

pub fn run<'a>(views: &'a Vec<View>, patches: &'a Vec<(u64, Patch)>) -> Result<HashMap<&'a str, Value>, &'static str> {
  let mut res : HashMap<&str, Value> = HashMap::new();

  for v in views {
    match proj(&v, patches) {
      Ok(val) => { res.insert(&v.field, val); },
      Err(err) => return Err(err)
    }
  }

  Ok(res)
}

fn proj<'a>(v: &'a View, px: &'a Vec<(u64, Patch)>) -> Result<Value, &'static str> {
  let patches = apply_filters(&v, px);

  match &v.projection {
    Projection::Latest => Ok(latest(&patches)),
    Projection::Collect => Ok(collect(&patches)),
    Projection::Avg => avg(&patches),
    Projection::Sum => sum(&patches),
    Projection::Concat(sep) => concat(&patches, &sep)
  }
}

fn apply_filters<'a>(view: &View, patches: &'a Vec<(u64, Patch)>) -> Vec<&'a Patch> {
  match &view.range {
    Some(Range { from, to }) => {
      patches
        .iter()
        .filter(|(t, p)| p.field == view.field && t >= from && t <= to)
        .map(|(_, p)| p)
        .collect()
    },
    None => {
      patches
        .iter()
        .filter(|(_, p)| p.field == view.field)
        .map(|(_, p)| p)
        .collect()
    }
  }
}

fn latest(patches: &Vec<&Patch>) -> Value {
  match patches.last() {
    Some(p) => serde_json::to_value(&p.value).unwrap(),
    None => serde_json::to_value(()).unwrap()
  }
}

fn collect(patches: &Vec<&Patch>) -> Value {
  let vals : Vec<&Val> = patches.iter().map(|p| &p.value).collect();
  serde_json::to_value(vals).unwrap()
}

fn avg(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let nx = numerics(patches);

  let len = nx.len();

  if len != patches.len() {
    Err("Cannot average non-numeric value stream")
  } else if len == 0 {
    Err("Cannot average empty value stream")
  } else {
    let res = nx.iter().sum::<f64>() / len as f64;
    Ok(serde_json::to_value(res).unwrap())
  }
}

fn sum(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let nx = numerics(patches);

  let len = nx.len();

  if len != patches.len() {
    Err("Cannot sum non-numeric value stream")
  } else {
    let res = nx.iter().sum::<f64>();
    Ok(serde_json::to_value(res).unwrap())
  }
}

fn concat(patches: &Vec<&Patch>, sep: &str) -> Result<Value, &'static str> {
  let sx = strings(patches);

  if sx.len() != patches.len() {
    Err("Cannot concat non-string value stream")
  } else {
    Ok(serde_json::to_value(sx.join(sep)).unwrap())
  }
}

fn numerics(patches: &Vec<&Patch>) -> Vec<f64> {
  patches.iter().filter_map(|p| p.value.as_num()).collect()
}

fn strings<'a>(patches: &'a Vec<&Patch>) -> Vec<&'a str> {
  patches.iter().filter_map(|p| p.value.as_str()).collect()
}
