
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

fn avg(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let nx = numerics(patches, "Cannot average non-numeric value stream")?;
  let res = nx.iter().sum::<f64>() / nx.len() as f64;
  Ok(serde_json::to_value(res).unwrap())
}

fn sum(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let nx = numerics(patches, "Cannot sum non-numeric value stream")?;
  let res = nx.iter().sum::<f64>();
  Ok(serde_json::to_value(res).unwrap())
}

// String projections

fn concat(patches: &Vec<&Patch>, sep: &str) -> Result<Value, &'static str> {
  let sx = strings(patches, "Cannot concat non-string value stream")?;
  Ok(serde_json::to_value(sx.join(sep)).unwrap())
}

// Boolean projections

fn all(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let bx = bools(patches, "Cannot apply conjunction to non-boolean value stream")?;
  let res = bx.iter().all(|b| *b);
  Ok(serde_json::to_value(res).unwrap())
}

fn any(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let bx = bools(patches, "Cannot apply disjunction to non-boolean value stream")?;
  let res = bx.iter().any(|b| *b);
  Ok(serde_json::to_value(res).unwrap())
}

fn none(patches: &Vec<&Patch>) -> Result<Value, &'static str> {
  let bx = bools(patches, "Cannot apply conjunction to non-boolean value stream")?;
  let res = bx.iter().all(|b| !(*b));
  Ok(serde_json::to_value(res).unwrap())
}

fn numerics(patches: &Vec<&Patch>, err: &'static str) -> Result<Vec<f64>, &'static str> {
  of_type(patches, |p| p.value.as_f64(), err)
}

fn strings<'a>(patches: &'a Vec<&Patch>, err: &'static str) -> Result<Vec<&'a str>, &'static str> {
  of_type(patches, |p| p.value.as_str(), err)
}

fn bools<'a>(patches: &'a Vec<&Patch>, err: &'static str) -> Result<Vec<bool>, &'static str> {
  of_type(patches, |p| p.value.as_bool(), err)
}

fn of_type<'a, R, F>(patches: &'a Vec<&Patch>, mut f: F, err: &'static str) -> Result<Vec<R>, &'static str> where F : FnMut(&'a Patch) -> Option<R> {
  let rx : Vec<R> = patches.iter().filter_map(|p| f(p)).collect();

  if rx.len() != patches.len() {
    Err(err)
  } else {
    Ok(rx)
  }
}
