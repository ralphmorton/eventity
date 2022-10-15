
use rocket::http;
use rocket::request;
use rocket::response;
use rocket::serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Patch {
  pub field: String,
  pub value: Value
}

#[derive(Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct View {
  pub field: String,
  pub alias: Option<String>,
  pub range: Option<Range>,
  pub projection: Projection
}

#[derive(Deserialize, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Range {
  pub from: u64,
  pub to: u64
}

#[derive(Deserialize, Serialize)]
#[serde(crate = "rocket::serde", tag = "t", content = "c")]
pub enum Projection {
  // Generic
  Latest,
  Collect,
  // Numeric
  Avg,
  Sum,
  // String
  Concat(String),
  // Boolean
  All,
  Any,
  None
}

pub struct ViewResult<'a> {
  field: &'a str,
  value: Value
}

impl<'a> ViewResult<'a> {
  pub fn create(field: &'a str, value: Value) -> Self {
    Self {
      field,
      value
    }
  }

  pub fn to_tuple(&self) -> (&'a str, Value) {
    (self.field, self.value.clone())
  }
}

pub enum ViewResponse {
  Success(Value),
  Error(String)
}

#[derive(Serialize)]
pub struct QueryError {
  error: String
}

impl ViewResponse {
  pub fn create(res: Result<HashMap<&str, Value>, String>) -> Self {
    match res {
      Ok(v) => Self::Success(serde_json::to_value(v).unwrap()),
      Err(error) => Self::Error(error)
    }
  }
}

impl<'a> response::Responder<'a, 'a> for ViewResponse {
  fn respond_to(self, _: &request::Request) -> response::Result<'a> {
    match self {
      Self::Success(v) => {
        let json = serde_json::to_string(&v).unwrap();

        response::Response::build()
          .header(http::ContentType::JSON)
          .status(http::Status::Ok)
          .sized_body(json.len(), Cursor::new(json))
          .ok()
      },
      Self::Error(e) => {
        let qe = QueryError { error: e };
        let json = serde_json::to_string(&qe).unwrap();

        response::Response::build()
          .header(http::ContentType::JSON)
          .status(http::Status::BadRequest)
          .sized_body(json.len(), Cursor::new(json))
          .ok()
      }
    }
  }
}
