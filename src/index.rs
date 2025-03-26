use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Index {
    name: String,
    fields: Vec<String>,
}

impl Index {
    pub fn new<S1: Into<String>, S2: Into<String>>(name: S1, fields: Vec<S2>) -> Self {
        Self {
            name: name.into(),
            fields: fields.into_iter().map(|f| f.into()).collect(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }
}
